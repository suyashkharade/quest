#!/usr/bin/env python3
"""
HTTP to S3 Sync Script
Syncs files from an HTTP base URL to an S3 directory.
Removes files from S3 that no longer exist at the HTTP source.
"""

import requests
import boto3
import hashlib
import logging
from urllib.parse import urljoin, urlparse
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Set, Dict, Optional
import argparse
import sys
from bs4 import BeautifulSoup
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HttpS3Sync:
    def __init__(self, http_base_url: str, s3_bucket: str, s3_prefix: str = "", 
                 aws_access_key: str = None, aws_secret_key: str = None, 
                 aws_region: str = "us-east-1"):
        """
        Initialize the sync client
        
        Args:
            http_base_url: Base HTTP URL to sync from
            s3_bucket: S3 bucket name
            s3_prefix: S3 key prefix (directory path)
            aws_access_key: AWS access key (optional, can use env vars or IAM)
            aws_secret_key: AWS secret key (optional, can use env vars or IAM)
            aws_region: AWS region
        """
        self.http_base_url = http_base_url.rstrip('/')
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.strip('/')
        
        # Initialize S3 client
        try:
            if aws_access_key and aws_secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
            else:
                self.s3_client = boto3.client('s3', region_name=aws_region)
        except NoCredentialsError:
            logger.error("AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or configure IAM role.")
            sys.exit(1)
        
        # Initialize session for HTTP requests
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; Screen: 1920x1080) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})

    def get_http_file_list(self) -> Set[str]:
        """
        Get list of files from HTTP directory listing
        This assumes the HTTP server provides a directory listing
        """
        try:
            response = self.session.get(self.http_base_url, timeout=30)
            response.raise_for_status()
            
            # Parse HTML to extract file links
            soup = BeautifulSoup(response.content, 'html.parser')
            files = set()

            
            # Look for links that appear to be files (not directories)
            for link in soup.find_all('a', href=True):
                href = link.get_text()
                print(link)
                # Skip parent directory links and directories
                if href.startswith('..') or href.endswith('/') or href.startswith('http'):
                    continue
                
                # Basic file detection (has extension)
                if '.' in href and not href.startswith('?'):
                    files.add(href)
            
            logger.info(f"Found {len(files)} files at HTTP source")
            return files
            
        except requests.RequestException as e:
            logger.error(f"Failed to get HTTP file list: {e}")
            return set()

    def get_s3_file_list(self) -> Dict[str, str]:
        """
        Get list of files from S3 with their ETags
        Returns dict: {filename: etag}
        """
        try:
            files = {}
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            prefix = f"{self.s3_prefix}/" if self.s3_prefix else ""
            
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Extract filename from full key
                        key = obj['Key']
                        if prefix:
                            filename = key[len(prefix):]
                        else:
                            filename = key
                        
                        # Skip if it's a directory marker or empty
                        if filename and not filename.endswith('/'):
                            files[filename] = obj['ETag'].strip('"')
            
            logger.info(f"Found {len(files)} files in S3")
            return files
            
        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            return {}

    def calculate_md5(self, content: bytes) -> str:
        """Calculate MD5 hash of content"""
        return hashlib.md5(content).hexdigest()

    def download_file(self, filename: str) -> Optional[bytes]:
        """Download file from HTTP source"""
        url = f"{self.http_base_url}/{filename}"
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.error(f"Failed to download {filename}: {e}")
            return None

    def upload_to_s3(self, filename: str, content: bytes) -> bool:
        """Upload file to S3"""
        s3_key = f"{self.s3_prefix}/{filename}" if self.s3_prefix else filename
        
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=content,
                ContentType=self.get_content_type(filename)
            )
            logger.info(f"Uploaded {filename} to S3")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {filename} to S3: {e}")
            return False

    def delete_from_s3(self, filename: str) -> bool:
        """Delete file from S3"""
        s3_key = f"{self.s3_prefix}/{filename}" if self.s3_prefix else filename
        
        try:
            self.s3_client.delete_object(Bucket=self.s3_bucket, Key=s3_key)
            logger.info(f"Deleted {filename} from S3")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete {filename} from S3: {e}")
            return False

    def get_content_type(self, filename: str) -> str:
        """Get content type based on file extension"""
        ext = filename.lower().split('.')[-1]
        content_types = {
            'txt': 'text/plain',
            'html': 'text/html',
            'css': 'text/css',
            'js': 'application/javascript',
            'json': 'application/json',
            'xml': 'application/xml',
            'pdf': 'application/pdf',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'zip': 'application/zip',
            'tar': 'application/x-tar',
            'gz': 'application/gzip'
        }
        return content_types.get(ext, 'application/octet-stream')

    def sync(self, dry_run: bool = False) -> bool:
        """
        Perform the sync operation
        
        Args:
            dry_run: If True, only show what would be done without actually doing it
        """
        logger.info(f"Starting sync from {self.http_base_url} to s3://{self.s3_bucket}/{self.s3_prefix}")
        
        # Get file lists
        http_files = self.get_http_file_list()
        s3_files = self.get_s3_file_list()
        
        if not http_files:
            logger.warning("No files found at HTTP source")
            return False
        
        # Files to upload (new or modified)
        to_upload = []
        # Files to delete (exist in S3 but not in HTTP source)
        to_delete = []
        
        # Check files that need uploading
        for filename in http_files:
            if dry_run:
                if filename not in s3_files:
                    logger.info(f"[DRY RUN] Would upload new file: {filename}")
                    to_upload.append(filename)
                else:
                    logger.info(f"[DRY RUN] Would check if {filename} needs updating")
                    to_upload.append(filename)  # In dry run, assume it might need updating
            else:
                # Download and check if upload is needed
                content = self.download_file(filename)
                if content is None:
                    continue
                
                content_md5 = self.calculate_md5(content)
                
                if filename not in s3_files or s3_files[filename] != content_md5:
                    if self.upload_to_s3(filename, content):
                        to_upload.append(filename)
        
        # Check files that need deleting
        for filename in s3_files:
            if filename not in http_files:
                if dry_run:
                    logger.info(f"[DRY RUN] Would delete: {filename}")
                    to_delete.append(filename)
                else:
                    if self.delete_from_s3(filename):
                        to_delete.append(filename)
        
        # Summary
        logger.info(f"Sync completed:")
        logger.info(f"  Files uploaded: {len(to_upload)}")
        logger.info(f"  Files deleted: {len(to_delete)}")
        
        return True

def main():
    
    
    logging.getLogger().setLevel(logging.INFO)
    
    # Create sync client
    sync_client = HttpS3Sync(
        http_base_url='https://download.bls.gov/pub/time.series/pr/',
        s3_bucket='bls-data-assgn',
        s3_prefix='data',
        aws_access_key='AKIAYSRBUYSI7CINTAUT',
        aws_secret_key='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',
        aws_region='ap-south-1'
    )
    success = sync_client.sync()
    logger.info(f"success:{success}")    

if __name__ == '__main__':
    main()
