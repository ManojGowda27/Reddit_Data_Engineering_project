import sys
import os
import csv
import logging
from datetime import datetime
from typing import Iterator, Dict, Any, List

import praw
from praw import Reddit

# --- NEW IMPORT ---
# We import the tools we just built in the other file
from pipelines.aws_s3_pipeline import connect_to_s3, create_bucket_if_not_exists, upload_to_s3

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

POST_FIELDS = ['id', 'title', 'score', 'num_comments', 'author', 'created_utc', 'url', 'over_18', 'edited', 'spoiler', 'stickied']

def connect_reddit(client_id: str, client_secret: str, user_agent: str) -> Reddit:
    """Creates a Reddit instance with error handling."""
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        _ = reddit.read_only
        logger.info("Successfully connected to Reddit API.")
        return reddit
    except Exception as e:
        logger.error(f"Failed to connect to Reddit: {e}")
        sys.exit(1)

def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit: int = None) -> Iterator[Dict[str, Any]]:
    """
    Generator function that streams posts one by one.
    Memory Usage: O(1)
    """
    subreddit_obj = reddit_instance.subreddit(subreddit)
    posts = subreddit_obj.top(time_filter=time_filter, limit=limit)

    logger.info(f"Starting extraction from r/{subreddit}...")

    try:
        for post in posts:
            post_data = {key: getattr(post, key, None) for key in POST_FIELDS}
            yield post_data
    except Exception as e:
        logger.error(f"Error during extraction stream: {e}")

def transform_post(post: Dict[str, Any]) -> Dict[str, Any]:
    """Pure Python transformation."""
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
    if isinstance(edited_val, bool):
        post['edited'] = edited_val
    else:
        post['edited'] = False 

    return post

def reddit_pipeline(file_name: str, subreddit: str, time_filter: str, limit: int = None, **kwargs):
    """
    Orchestrates Extract -> Transform -> Load (S3).
    """
    # 1. Get Reddit Credentials
    CLIENT_ID = kwargs.get('client_id')
    CLIENT_SECRET = kwargs.get('client_secret')
    USER_AGENT = kwargs.get('user_agent', 'script:v1.0')

    # 2. Get AWS Credentials (Passed from Airflow)
    AWS_KEY_ID = kwargs.get('aws_access_key_id')
    AWS_SECRET_KEY = kwargs.get('aws_secret_access_key')
    BUCKET_NAME = kwargs.get('bucket_name')

    # Connect to Reddit
    reddit = connect_reddit(CLIENT_ID, CLIENT_SECRET, USER_AGENT)

    # Temporary local path
    file_path = f"/tmp/{file_name}.csv"
    post_stream = extract_posts(reddit, subreddit, time_filter, limit)

    try:
        # --- PHASE 1: WRITE TO LOCAL CSV ---
        logger.info(f"Writing data locally to {file_path}...")
        with open(file_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=POST_FIELDS)
            writer.writeheader()
            count = 0
            for post in post_stream:
                clean_post = transform_post(post)
                writer.writerow(clean_post)
                count += 1
        logger.info(f"Saved {count} rows to {file_path}.")

        # --- PHASE 2: UPLOAD TO S3 ---
        logger.info("Starting S3 Upload...")
        s3 = connect_to_s3(AWS_KEY_ID, AWS_SECRET_KEY)
        
        # Optional: ensure bucket exists
        create_bucket_if_not_exists(s3, BUCKET_NAME)
        
        # Upload the file we just created
        upload_to_s3(s3, file_path, BUCKET_NAME, f"{file_name}.csv")
        
        # --- PHASE 3: CLEANUP ---
        # Delete the local file so the Docker container doesn't fill up
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Local file {file_path} deleted successfully.")

        return f"s3://{BUCKET_NAME}/raw/{file_name}.csv"

    except IOError as e:
        logger.error(f"Pipeline Error: {e}")
        raise e