"""
S3 Operations Module using boto3

This module replaces subprocess calls to AWS CLI with native boto3 operations.
Provides the same functionality as the original CLI-based functions but with
better error handling, performance, and progress tracking.

Optimized for high-performance downloads with increased connection pooling.
"""

import os
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
from boto3_auth import get_s3_client, ensure_valid_credentials


class ProgressCallback:
    """Progress callback for S3 operations"""
    
    def __init__(self, filename, total_size):
        self.filename = filename
        self.total_size = total_size
        self.bytes_transferred = 0
        self.last_update = datetime.now()
    
    def __call__(self, bytes_amount):
        """Called by boto3 during transfer"""
        self.bytes_transferred += bytes_amount
        now = datetime.now()
        
        # Update every 5 seconds or on completion
        if (now - self.last_update).total_seconds() >= 5 or self.bytes_transferred >= self.total_size:
            if self.total_size > 0:
                percent = (self.bytes_transferred / self.total_size) * 100
                print(f"[{now}] {self.filename}: {percent:.1f}% ({self.bytes_transferred}/{self.total_size} bytes)")
            self.last_update = now


def list_bucket_contents_boto3(bucket_name, profile_name="dc3-cta"):
    """
    List all items in a bucket (both files and folders) using boto3
    
    Args:
        bucket_name (str): Name of the S3 bucket
        profile_name (str): AWS profile name
    
    Returns:
        list: List of tuples (item_name, item_type) where item_type is 'file' or 'folder'
    """
    try:
        s3_client = get_s3_client(profile_name)
        
        # Use paginator for large buckets
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Delimiter='/'
        )
        
        items = []
        
        for page in page_iterator:
            # Handle common prefixes (folders)
            for prefix in page.get('CommonPrefixes', []):
                folder_name = prefix['Prefix'].rstrip('/')
                items.append((folder_name, 'folder'))
            
            # Handle objects (files) at root level
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Skip if it's a folder marker (ends with /)
                if not key.endswith('/'):
                    items.append((key, 'file'))
        
        return items
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"Error listing bucket {bucket_name}: {error_code} - {e}")
        return []
    except Exception as e:
        print(f"Error listing bucket {bucket_name}: {e}")
        return []


def get_detailed_file_listing_boto3(bucket_name, item_name, item_type, profile_name="dc3-cta"):
    """
    Get detailed file listing for an item (file or folder) using boto3
    
    Args:
        bucket_name (str): Name of the S3 bucket
        item_name (str): Name of the item (file or folder)
        item_type (str): Type of item ('file' or 'folder')
        profile_name (str): AWS profile name
    
    Returns:
        list: List of dictionaries with file information
    """
    files = []
    
    try:
        s3_client = get_s3_client(profile_name)
        
        if item_type == 'file':
            # Single file - get object metadata
            try:
                response = s3_client.head_object(Bucket=bucket_name, Key=item_name)
                files.append({
                    'filename': os.path.basename(item_name),
                    'size': response['ContentLength'],
                    'path': item_name
                })
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') != 'NoSuchKey':
                    print(f"Error getting metadata for {bucket_name}/{item_name}: {e}")
        
        else:  # folder
            # Recursive folder listing using paginator
            prefix = item_name if item_name.endswith('/') else item_name + '/'
            
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    filename = os.path.basename(key)
                    
                    # Skip empty filenames or directory markers
                    if not filename or not filename.strip() or key.endswith('/'):
                        continue
                    
                    files.append({
                        'filename': filename,
                        'size': obj['Size'],
                        'path': key
                    })
    
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"Error getting detailed listing for {bucket_name}/{item_name}: {error_code} - {e}")
    except Exception as e:
        print(f"Error getting detailed listing for {bucket_name}/{item_name}: {e}")
    
    return files


def download_file_boto3(bucket_name, key, local_path, show_progress=True, profile_name="dc3-cta", 
                        progress_callback=None):
    """
    Download a file from S3 using boto3
    
    Args:
        bucket_name (str): Name of the S3 bucket
        key (str): S3 object key
        local_path (str): Local file path to save to
        show_progress (bool): Whether to show download progress
        profile_name (str): AWS profile name
        progress_callback: Custom progress callback function
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        s3_client = get_s3_client(profile_name)
        
        # Get object size for progress tracking
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=key)
            file_size = response['ContentLength']
        except ClientError:
            file_size = 0
        
        # Use custom callback if provided, otherwise create default one
        callback = None
        if progress_callback:
            callback = progress_callback
        elif show_progress and file_size > 0:
            callback = ProgressCallback(os.path.basename(local_path), file_size)
        
        # Download the file
        s3_client.download_file(
            Bucket=bucket_name,
            Key=key,
            Filename=local_path,
            Callback=callback
        )
        
        return True
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"Error downloading {bucket_name}/{key}: {error_code} - {e}")
        return False
    except Exception as e:
        print(f"Error downloading {bucket_name}/{key}: {e}")
        return False


def check_bucket_access_boto3(bucket_name, profile_name="dc3-cta"):
    """
    Check if we have access to a bucket
    
    Args:
        bucket_name (str): Name of the S3 bucket
        profile_name (str): AWS profile name
    
    Returns:
        bool: True if accessible, False otherwise
    """
    try:
        s3_client = get_s3_client(profile_name)
        
        # Try to list objects with limit 1
        s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        return True
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"No access to bucket {bucket_name}: {error_code}")
        return False
    except Exception as e:
        print(f"Error checking bucket access {bucket_name}: {e}")
        return False


def get_object_metadata_boto3(bucket_name, key, profile_name="dc3-cta"):
    """
    Get metadata for an S3 object
    
    Args:
        bucket_name (str): Name of the S3 bucket
        key (str): S3 object key
        profile_name (str): AWS profile name
    
    Returns:
        dict: Object metadata or None if error
    """
    try:
        s3_client = get_s3_client(profile_name)
        response = s3_client.head_object(Bucket=bucket_name, Key=key)
        
        return {
            'size': response['ContentLength'],
            'last_modified': response['LastModified'],
            'etag': response['ETag'].strip('"'),
            'content_type': response.get('ContentType', 'unknown')
        }
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code != 'NoSuchKey':  # Don't log missing keys as errors
            print(f"Error getting metadata for {bucket_name}/{key}: {error_code} - {e}")
        return None
    except Exception as e:
        print(f"Error getting metadata for {bucket_name}/{key}: {e}")
        return None


def format_boto3_error(error):
    """
    Format boto3 ClientError for user-friendly display
    
    Args:
        error (ClientError): The boto3 ClientError
    
    Returns:
        str: Formatted error message
    """
    if isinstance(error, ClientError):
        error_code = error.response.get('Error', {}).get('Code', 'Unknown')
        error_message = error.response.get('Error', {}).get('Message', str(error))
        return f"{error_code}: {error_message}"
    else:
        return str(error)
