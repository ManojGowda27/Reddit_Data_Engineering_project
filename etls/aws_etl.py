import sys
import logging
import s3fs

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def connect_to_s3(aws_access_key: str, aws_secret_key: str) -> s3fs.S3FileSystem:
    """
    Establishes a connection to S3 via s3fs.
    """
    try:
        s3 = s3fs.S3FileSystem(
            anon=False,
            key=aws_access_key,
            secret=aws_secret_key
        )
        return s3
    except Exception as e:
        logger.error(f"Failed to connect to S3: {e}")
        raise e

def create_bucket_if_not_exists(s3: s3fs.S3FileSystem, bucket: str):
    """
    Checks and creates bucket.
    """
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            logger.info(f"Bucket '{bucket}' created.")
        else:
            logger.info(f"Bucket '{bucket}' already exists.")
    except Exception as e:
        logger.error(f"Error checking/creating bucket '{bucket}': {e}")
        raise e

def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket: str, s3_file_name: str):
    """
    Uploads a local file to the 'raw' folder in S3.
    """
    s3_dest_path = f"{bucket}/raw/{s3_file_name}"
    
    try:
        logger.info(f"Uploading {file_path} to s3://{s3_dest_path}...")
        s3.put(file_path, s3_dest_path)
        logger.info("Upload successful.")
    except FileNotFoundError:
        logger.error(f"Local file not found: {file_path}")
        raise FileNotFoundError(f"The file {file_path} was not found.")
    except Exception as e:
        logger.error(f"S3 Upload failed: {e}")
        raise e