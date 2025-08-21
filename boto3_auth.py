"""
AWS SSO Authentication Module for boto3

This module handles AWS SSO authentication and session management for boto3,
replacing the subprocess calls to aws cli for authentication.
"""

import os
import boto3
from botocore.exceptions import (
    ClientError, 
    NoCredentialsError, 
    TokenRetrievalError, 
    UnauthorizedSSOTokenError,
    SSOTokenLoadError
)
from botocore.session import Session
from botocore.config import Config
import subprocess
from datetime import datetime


class SSOManager:
    """Manages AWS SSO authentication for boto3 sessions"""
    
    def __init__(self, profile_name="default"):
        self.profile_name = profile_name
        self.session = None
        self.s3_client = None
        self._initialize_session()
    
    def _initialize_session(self):
        """Initialize boto3 session with SSO profile"""
        try:
            # Create session with specified profile
            self.session = boto3.Session(profile_name=self.profile_name)
            print(f"[{datetime.now()}] Initialized session with profile: {self.profile_name}")
        except Exception as e:
            print(f"[{datetime.now()}] Error initializing session: {e}")
            # Fallback to default session
            self.session = boto3.Session()
    
    def get_s3_client(self, force_refresh=False):
        """Get S3 client with optimized configuration, refreshing if needed"""
        if self.s3_client is None or force_refresh:
            try:
                # Create optimized configuration for high-performance downloads
                config = Config(
                    # Connection pool optimization - CRITICAL for performance!
                    max_pool_connections=100,  # Default is only 10, this increases concurrent connections
                    
                    # Retry configuration
                    retries={'max_attempts': 3, 'mode': 'adaptive'},
                    
                    # Timeout optimizations
                    connect_timeout=10,  # Connection timeout
                    read_timeout=30,     # Read timeout for large files
                    
                    # Performance optimizations
                    tcp_keepalive=True,           # Keep connections alive
                    parameter_validation=False,   # Skip parameter validation for speed
                    
                    # S3 specific optimizations
                    s3={
                        'addressing_style': 'virtual'     # Use virtual hosted-style addressing
                        # Note: Transfer acceleration removed as it can cause InvalidRequest errors
                        # on buckets that don't have it enabled
                    }
                )
                
                self.s3_client = self.session.client('s3', config=config)
                # Test the client with a simple operation
                self.s3_client.list_buckets()
                print(f"[{datetime.now()}] Optimized S3 client ready (max_pool_connections=100)")
            except (TokenRetrievalError, UnauthorizedSSOTokenError, SSOTokenLoadError) as e:
                print(f"[{datetime.now()}] SSO token issue: {e}")
                if self._refresh_sso_token():
                    # Recreate optimized client after token refresh
                    config = Config(
                        max_pool_connections=100,
                        retries={'max_attempts': 3, 'mode': 'adaptive'},
                        connect_timeout=10,
                        read_timeout=30,
                        tcp_keepalive=True,
                        parameter_validation=False,
                        s3={
                            'addressing_style': 'virtual'
                        }
                    )
                    self.s3_client = self.session.client('s3', config=config)
                else:
                    raise
            except NoCredentialsError:
                print(f"[{datetime.now()}] No credentials found. Please configure AWS SSO.")
                raise
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                if error_code in ['UnauthorizedOperation', 'InvalidUserID.NotFound']:
                    print(f"[{datetime.now()}] Authorization error: {e}")
                    if self._refresh_sso_token():
                        # Recreate optimized client after token refresh
                        config = Config(
                            max_pool_connections=100,
                            retries={'max_attempts': 3, 'mode': 'adaptive'},
                            connect_timeout=10,
                            read_timeout=30,
                            tcp_keepalive=True,
                            parameter_validation=False,
                            s3={
                                'addressing_style': 'virtual'
                            }
                        )
                        self.s3_client = self.session.client('s3', config=config)
                    else:
                        raise
                else:
                    raise
        
        return self.s3_client
    
    def _refresh_sso_token(self):
        """Refresh SSO token using AWS CLI"""
        print(f"[{datetime.now()}] Attempting to refresh SSO token...")
        try:
            # Use AWS CLI to refresh SSO token
            subprocess.run([
                "aws", "sso", "login", 
                "--profile", self.profile_name
            ], check=True, capture_output=False)
            
            print(f"[{datetime.now()}] SSO token refreshed successfully")
            
            # Reinitialize session after token refresh
            self._initialize_session()
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"[{datetime.now()}] Failed to refresh SSO token: {e}")
            return False
        except FileNotFoundError:
            print(f"[{datetime.now()}] AWS CLI not found. Please install AWS CLI.")
            return False
    
    def check_credentials(self):
        """Check if current credentials are valid"""
        try:
            sts_client = self.session.client('sts')
            response = sts_client.get_caller_identity()
            print(f"[{datetime.now()}] Authenticated as: {response.get('Arn', 'Unknown')}")
            return True
        except (TokenRetrievalError, UnauthorizedSSOTokenError, SSOTokenLoadError):
            print(f"[{datetime.now()}] SSO token expired or invalid")
            return False
        except NoCredentialsError:
            print(f"[{datetime.now()}] No credentials configured")
            return False
        except ClientError as e:
            print(f"[{datetime.now()}] Error checking credentials: {e}")
            return False
    
    def ensure_valid_session(self):
        """Ensure we have a valid session, refreshing if necessary"""
        if not self.check_credentials():
            print(f"[{datetime.now()}] Invalid credentials, attempting refresh...")
            if self._refresh_sso_token():
                return self.check_credentials()
            else:
                return False
        return True


# Global SSO manager instances (one per profile)
_sso_managers = {}

def get_sso_manager(profile_name="default"):
    """Get the SSO manager instance for the specified profile"""
    global _sso_managers
    if profile_name not in _sso_managers:
        _sso_managers[profile_name] = SSOManager(profile_name)
    return _sso_managers[profile_name]

def get_s3_client(profile_name="default", force_refresh=False):
    """Get an authenticated S3 client"""
    sso_manager = get_sso_manager(profile_name)
    return sso_manager.get_s3_client(force_refresh=force_refresh)

def ensure_valid_credentials(profile_name="default"):
    """Ensure we have valid AWS credentials"""
    sso_manager = get_sso_manager(profile_name)
    return sso_manager.ensure_valid_session()
