from etls.reddit_etl import reddit_pipeline_logic

def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None, **kwargs):
    # Just a wrapper that calls the atomic logic in the ETL folder
    # We pass **kwargs through because that's where the secrets are
    return reddit_pipeline_logic(file_name, subreddit, time_filter, limit, **kwargs)