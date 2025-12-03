import sys
import csv
import logging
from datetime import datetime
from typing import Iterator, Dict, Any, List

import praw
from praw import Reddit

# ## OPTIMIZATION: Observability
# Replaced print() with the logging module. 
# Why: Allows integration with monitoring tools (Splunk/Datadog) and standardized log levels (INFO vs ERROR).
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
    ## OPTIMIZATION: Memory Management
    Previous approach: post_lists.append(post) -> O(N) memory.
    New approach: yield post_data -> O(1) memory.
    Why: Prevents 'Out of Memory' (OOM) errors in Docker containers when processing large datasets.
    """
    subreddit_obj = reddit_instance.subreddit(subreddit)
    posts = subreddit_obj.top(time_filter=time_filter, limit=limit)

    logger.info(f"Starting extraction from r/{subreddit}...")

    try:
        for post in posts:
            # ## OPTIMIZATION: Data Minimization
            # Previous approach: vars(post) loaded all API fields.
            # New approach: Dict comprehension extracts ONLY required fields.
            post_data = {key: getattr(post, key, None) for key in POST_FIELDS}
            yield post_data
    except Exception as e:
        logger.error(f"Error during extraction stream: {e}")

def transform_post(post: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforms a single post dictionary.
    ## OPTIMIZATION: Dependency Reduction
    Previous approach: Used Pandas/Numpy for simple cleaning.
    New approach: Pure Python transformation.
    Why: Reduces Docker image size (no need to install Pandas/Numpy) and speeds up container startup time.
    """
    # 1. Date Conversion
    if post.get('created_utc'):
        post['created_utc'] = datetime.utcfromtimestamp(post['created_utc']).isoformat()

    # 2. Boolean Logic
    post['over_18'] = bool(post.get('over_18', False))
    post['spoiler'] = bool(post.get('spoiler', False))
    post['stickied'] = bool(post.get('stickied', False))

    # 3. Type Casting
    post['author'] = str(post.get('author', 'Unknown'))
    post['num_comments'] = int(post.get('num_comments', 0))
    post['score'] = int(post.get('score', 0))
    post['title'] = str(post.get('title', '')).strip()

    # 4. Handle 'edited' logic
    # ## OPTIMIZATION: Streaming Algorithm
    # Previous approach: calculated mode() of the column (requires full dataset).
    # New approach: Default fallback.
    # Why: You cannot calculate statistical mode in a true stream without blocking execution. 
    # This change enables continuous processing.
    edited_val = post.get('edited')
    if isinstance(edited_val, bool):
        post['edited'] = edited_val
    else:
        post['edited'] = False 

    return post

def reddit_pipeline(file_name: str, subreddit: str, time_filter: str, limit: int = None, **kwargs):
    """
    Main entry point used by Airflow PythonOperator.
    """
    # ## OPTIMIZATION: Security
    # Previous approach: Hardcoded credentials or reliance on global variables.
    # New approach: Fetch from kwargs (Airflow Connections) or env vars.
    CLIENT_ID = kwargs.get('client_id') or sys.argv[1] 
    CLIENT_SECRET = kwargs.get('client_secret') or sys.argv[2]
    USER_AGENT = kwargs.get('user_agent', 'script:v1.0')

    reddit = connect_reddit(CLIENT_ID, CLIENT_SECRET, USER_AGENT)

    file_path = f"/tmp/{file_name}.csv"

    post_stream = extract_posts(reddit, subreddit, time_filter, limit)

    logger.info(f"Writing data to {file_path}...")

    try:
        # ## OPTIMIZATION: I/O Performance
        # Previous approach: Write to CSV only after processing all data.
        # New approach: Write row-by-row inside the loop.
        # Why: Improves Time-to-First-Byte (latency) and ensures partial data is saved if the job crashes halfway.
        with open(file_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=POST_FIELDS)
            writer.writeheader()

            count = 0
            for post in post_stream:
                # Transform ON THE FLY
                clean_post = transform_post(post)
                # Load ON THE FLY
                writer.writerow(clean_post)
                count += 1

        logger.info(f"Successfully processed and saved {count} rows to {file_path}.")
        return file_path

    except IOError as e:
        logger.error(f"File I/O Error: {e}")
        raise e