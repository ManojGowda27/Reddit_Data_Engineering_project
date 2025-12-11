import sys
import os
import csv
import logging
from datetime import datetime
from typing import Iterator, Dict, Any

import praw
from praw import Reddit

# Import S3 tools
from etls.aws_etl import connect_to_s3, create_bucket_if_not_exists, upload_to_s3

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

POST_FIELDS = ['id', 'title', 'score', 'num_comments', 'author', 'created_utc', 'url', 'over_18', 'edited', 'spoiler', 'stickied']

class RedditClient:
    """
    Client to interact with Reddit API.
    Handles authentication and data streaming.
    """
    def __init__(self, client_id: str, client_secret: str, user_agent: str):
        try:
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent
            )
            # Verify connection
            _ = self.reddit.read_only
            logger.info("RedditClient: Connected to Reddit API successfully.")
        except Exception as e:
            logger.error(f"RedditClient: Connection failed: {e}")
            sys.exit(1)

    def extract_posts(self, subreddit: str, time_filter: str, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Streams posts from a subreddit using the internal connection.
        """
        try:
            subreddit_obj = self.reddit.subreddit(subreddit)
            posts = subreddit_obj.top(time_filter=time_filter, limit=limit)

            logger.info(f"RedditClient: Streaming posts from r/{subreddit}...")

            for post in posts:
                # Extract only necessary fields safely
                yield {key: getattr(post, key, None) for key in POST_FIELDS}
                
        except Exception as e:
            logger.error(f"RedditClient: Extraction error: {e}")
            raise e

def transform_post(post: Dict[str, Any]) -> Dict[str, Any]:
    """Pure Python Transformation Logic"""
    if post.get('created_utc'):
        post['created_utc'] = datetime.utcfromtimestamp(post['created_utc']).isoformat()
    
    post['over_18'] = bool(post.get('over_18', False))
    post['spoiler'] = bool(post.get('spoiler', False))
    post['stickied'] = bool(post.get('stickied', False))
    post['author'] = str(post.get('author', 'Unknown'))
    post['num_comments'] = int(post.get('num_comments', 0))
    post['score'] = int(post.get('score', 0))
    post['title'] = str(post.get('title', '')).strip()
    
    edited_val = post.get('edited')
    post['edited'] = edited_val if isinstance(edited_val, bool) else False
    return post

def reddit_pipeline_logic(file_name: str, subreddit: str, time_filter: str, limit: int = None, **kwargs):
    """
    Orchestrates the pipeline using the RedditClient class.
    """
    # 1. Fetch Credentials
    CLIENT_ID = kwargs.get('client_id')
    CLIENT_SECRET = kwargs.get('client_secret')
    USER_AGENT = kwargs.get('user_agent', 'script:v1.0')
    AWS_KEY_ID = kwargs.get('aws_access_key_id')
    AWS_SECRET_KEY = kwargs.get('aws_secret_access_key')
    BUCKET_NAME = kwargs.get('bucket_name')

    # 2. Initialize Client (OOP Approach)
    client = RedditClient(CLIENT_ID, CLIENT_SECRET, USER_AGENT)
    
    file_path = f"/tmp/{file_name}.csv"

    try:
        # 3. Use Client to stream data
        post_stream = client.extract_posts(subreddit, time_filter, limit)

        # Write to Local CSV
        with open(file_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=POST_FIELDS)
            writer.writeheader()
            count = 0
            for post in post_stream:
                clean_post = transform_post(post)
                writer.writerow(clean_post)
                count += 1
        logger.info(f"Saved {count} rows locally to {file_path}.")

        # Upload to S3
        if AWS_KEY_ID and BUCKET_NAME:
            s3 = connect_to_s3(AWS_KEY_ID, AWS_SECRET_KEY)
            create_bucket_if_not_exists(s3, BUCKET_NAME)
            upload_to_s3(s3, file_path, BUCKET_NAME, f"{file_name}.csv")
            
        # Cleanup
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info("Local temp file deleted.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise e