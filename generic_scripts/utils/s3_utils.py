from urllib import response
from urllib.parse import urlparse
import boto3
s3 = boto3.client('s3')


def parse_s3_path(s3_path):
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key


def read_s3_path_data(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    # Read the content
    content_bytes = response['Body'].read()
    # Decode bytes to string (e.g., UTF-8)
    content_str = content_bytes.decode('utf-8').strip()
    return content_str
    
def write_s3_path(bucket, key, file_path):
    s3.upload_file(Filename=file_path, Bucket=bucket, Key=key)
    
def delete_s3_path_data(bucket, key):
    s3.delete_object(bucket, key)
    
def file_exists(bucket_name, key):
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except Exception as e:
        if e:
            return False
        raise        
    