import os
import sys
import time
import argparse
import re
import xml.etree.ElementTree as ET
import threading
import queue
from datetime import datetime
from pathlib import Path
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import our new boto3 modules
from boto3_auth import ensure_valid_credentials, get_s3_client
from s3_operations import (
    list_bucket_contents_boto3,
    get_detailed_file_listing_boto3,
    download_file_boto3,
    check_bucket_access_boto3,
    format_boto3_error
)
from progress_monitor import ProgressMonitor, WorkerStatus, EnhancedProgressCallback
from botocore.exceptions import ClientError, NoCredentialsError

profile = "dc3-cta"

class DownloadStatus(Enum):
    PENDING = "pending"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"

class ManifestManager:
    def __init__(self, manifest_file="download_manifest.xml"):
        self.manifest_file = manifest_file
        self.lock = threading.Lock()
        self.root = None
        self.load_or_create_manifest()
    
    def load_or_create_manifest(self):
        """Load existing manifest or create new one"""
        try:
            if os.path.exists(self.manifest_file):
                tree = ET.parse(self.manifest_file)
                self.root = tree.getroot()
                print(f"Loaded existing manifest with {len(self.root)} items")
            else:
                self.root = ET.Element("downloads")
                print("Created new manifest")
        except ET.ParseError:
            print("Corrupt manifest file, creating new one")
            self.root = ET.Element("downloads")
    
    def add_item(self, bucket, folder, filename, size_bytes, file_path=""):
        """Add item to manifest"""
        with self.lock:
            # Check if item already exists
            existing = self.find_item(bucket, folder, filename)
            if existing is not None:
                return existing
            
            item = ET.SubElement(self.root, "item")
            item.set("bucket", bucket)
            item.set("folder", folder)
            item.set("filename", filename)
            item.set("size", str(size_bytes))
            item.set("status", DownloadStatus.PENDING.value)
            item.set("file_path", file_path)
            item.set("added", datetime.now().isoformat())
            
            self.save_manifest()
            return item
    
    def find_item(self, bucket, folder, filename):
        """Find existing item in manifest"""
        for item in self.root:
            if (item.get("bucket") == bucket and 
                item.get("folder") == folder and 
                item.get("filename") == filename):
                return item
        return None
    
    def update_status(self, item, status, error_msg=""):
        """Update item status"""
        with self.lock:
            item.set("status", status.value)
            item.set("last_updated", datetime.now().isoformat())
            if error_msg:
                item.set("error", error_msg)
            elif "error" in item.attrib:
                del item.attrib["error"]
            self.save_manifest()
    
    def get_pending_items(self, include_failed=False):
        """Get all pending items, optionally including failed items for retry"""
        with self.lock:
            if include_failed:
                return [item for item in self.root if item.get("status") in [
                    DownloadStatus.PENDING.value, 
                    DownloadStatus.FAILED.value
                ]]
            else:
                return [item for item in self.root if item.get("status") == DownloadStatus.PENDING.value]
    
    def get_stats(self):
        """Get download statistics"""
        stats = {status.value: 0 for status in DownloadStatus}
        total_size = 0
        completed_size = 0
        
        for item in self.root:
            status = item.get("status")
            size = int(item.get("size", 0))
            stats[status] = stats.get(status, 0) + 1
            total_size += size
            if status == DownloadStatus.COMPLETED.value:
                completed_size += size
        
        return stats, total_size, completed_size
    
    def save_manifest(self):
        """Save manifest to file"""
        tree = ET.ElementTree(self.root)
        ET.indent(tree, space="  ", level=0)
        tree.write(self.manifest_file, encoding="utf-8", xml_declaration=True)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='S3Swarm - Orchestrated S3 data collection with worker swarm (boto3 version)')
    parser.add_argument('--destination', type=str, default='./s3_downloads',
                       help='Destination directory for downloads (default: ./s3_downloads)')
    parser.add_argument('--buckets-file', type=str, default='buckets.txt',
                       help='Text file containing bucket names (one per line)')
    parser.add_argument('--manifest', type=str, default='download_manifest.xml',
                       help='Manifest file path')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='Maximum concurrent downloads (default: 4)')
    parser.add_argument('--generate-manifest', action='store_true',
                       help='Only generate manifest, do not download')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be downloaded')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='Maximum retries per file (default: 3)')
    parser.add_argument('--retry-failed', action='store_true',
                       help='Include failed items in download queue for retry')
    parser.add_argument('--profile', type=str, default="dc3-cta",
                       help='AWS profile name (default: dc3-cta)')
    return parser.parse_args()

def load_buckets_from_file(buckets_file):
    """Load bucket names from text file"""
    if not os.path.exists(buckets_file):
        print(f"Creating example buckets file: {buckets_file}")
        with open(buckets_file, 'w') as f:
            f.write("dc3cta-handoff-exports\n")
            f.write("dc3cta-resources\n")
            f.write("dc3cta-ovf-imports\n")
        print(f"Please edit {buckets_file} with your bucket names and run again")
        return []
    
    buckets = []
    with open(buckets_file, 'r') as f:
        for line in f:
            bucket = line.strip()
            if bucket and not bucket.startswith('#'):
                buckets.append(bucket)
    
    return buckets

def format_size(bytes_size):
    """Format bytes to human readable size"""
    if bytes_size == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    i = 0
    while bytes_size >= 1024 and i < len(units) - 1:
        bytes_size /= 1024
        i += 1
    
    return f"{bytes_size:.1f} {units[i]}"

def list_bucket_contents(bucket_name, profile_name):
    """List all items in a bucket using boto3"""
    return list_bucket_contents_boto3(bucket_name, profile_name)

def get_detailed_file_listing(bucket_name, item_name, item_type, profile_name):
    """Get detailed file listing using boto3"""
    return get_detailed_file_listing_boto3(bucket_name, item_name, item_type, profile_name)

def generate_manifest(buckets, manifest_manager, profile_name):
    """Generate complete manifest of all files to download"""
    print(f"[{datetime.now()}] Generating manifest for {len(buckets)} buckets...")
    
    total_items = 0
    total_size = 0
    
    for bucket in buckets:
        print(f"\n[{datetime.now()}] Processing bucket: {bucket}")
        
        # Check bucket access first
        if not check_bucket_access_boto3(bucket, profile_name):
            print(f"  Skipping bucket {bucket} - no access")
            continue
        
        # Get bucket contents
        items = list_bucket_contents(bucket, profile_name)
        print(f"  Found {len(items)} items")
        
        for item_name, item_type in items:
            print(f"  Analyzing {item_type}: {item_name}")
            
            # Get detailed file listing
            files = get_detailed_file_listing(bucket, item_name, item_type, profile_name)
            
            for file_info in files:
                # Skip empty filenames or zero-size entries that aren't valid files
                if not file_info['filename'] or not file_info['filename'].strip():
                    continue
                
                manifest_manager.add_item(
                    bucket=bucket,
                    folder=item_name,
                    filename=file_info['filename'],
                    size_bytes=file_info['size'],
                    file_path=file_info['path']
                )
                total_items += 1
                total_size += file_info['size']
            
            print(f"    Added {len(files)} files ({format_size(sum(f['size'] for f in files))})")
    
    print(f"\n[{datetime.now()}] Manifest generation complete!")
    print(f"Total items: {total_items}")
    print(f"Total size: {format_size(total_size)}")
    
    return total_items, total_size

def create_lock_file(dest_path, filename):
    """Create lock file for concurrent download protection"""
    lock_file = os.path.join(dest_path, f"{filename}.lock")
    try:
        with open(lock_file, 'w') as f:
            f.write(f"Locked by process {os.getpid()} at {datetime.now().isoformat()}")
        return lock_file
    except Exception:
        return None

def remove_lock_file(lock_file):
    """Remove lock file"""
    try:
        if os.path.exists(lock_file):
            os.remove(lock_file)
    except Exception:
        pass

def check_lock_file(dest_path, filename):
    """Check if lock file exists"""
    lock_file = os.path.join(dest_path, f"{filename}.lock")
    return os.path.exists(lock_file)

def download_single_file(item, base_dest_path, manifest_manager, max_retries, profile_name, progress_monitor=None, worker_id=None):
    """Download a single file with lock file protection using boto3"""
    bucket = item.get("bucket")
    folder = item.get("folder")
    filename = item.get("filename")
    file_path = item.get("file_path")
    size_bytes = int(item.get("size", 0))
    
    # Skip invalid entries (empty filenames, directories, etc.)
    if not filename or not filename.strip():
        if progress_monitor and worker_id:
            progress_monitor.update_worker_status(worker_id, WorkerStatus.FAILED, error="Invalid entry - empty filename")
        print(f"[{datetime.now()}] Skipping invalid entry with empty filename in {bucket}/{folder}")
        manifest_manager.update_status(item, DownloadStatus.FAILED, "Invalid entry - empty filename")
        return False
    
    # Determine source and destination paths
    if file_path and file_path != filename:
        # File is in a subfolder structure
        source_key = file_path
        # Clean up the path by removing the folder prefix and getting the directory
        relative_path = file_path.replace(folder + '/', '', 1) if file_path.startswith(folder + '/') else file_path
        subdir = os.path.dirname(relative_path)
        if subdir:
            dest_dir = os.path.join(base_dest_path, bucket, folder, subdir)
        else:
            dest_dir = os.path.join(base_dest_path, bucket, folder)
        dest_file = os.path.join(dest_dir, filename)
    else:
        # File is directly in the folder or is the folder itself
        source_key = folder if folder != filename else filename
        dest_dir = os.path.join(base_dest_path, bucket)
        dest_file = os.path.join(dest_dir, filename)
    
    # Normalize the paths to handle any double slashes or trailing slashes
    dest_dir = os.path.normpath(dest_dir)
    dest_file = os.path.normpath(dest_file)
    
    # Create destination directory
    try:
        os.makedirs(dest_dir, exist_ok=True)
    except OSError as e:
        print(f"[{datetime.now()}] Error creating directory {dest_dir}: {e}")
        return False
    
    # Check for existing lock file
    if check_lock_file(dest_dir, filename):
        if progress_monitor and worker_id:
            progress_monitor.update_worker_status(worker_id, WorkerStatus.IDLE)
        print(f"[{datetime.now()}] Skipping {filename} - lock file exists")
        return False
    
    # Create lock file
    lock_file = create_lock_file(dest_dir, filename)
    if not lock_file:
        if progress_monitor and worker_id:
            progress_monitor.update_worker_status(worker_id, WorkerStatus.FAILED, error="Could not create lock file")
        print(f"[{datetime.now()}] Could not create lock file for {filename}")
        return False
    
    try:
        # Update status to started
        manifest_manager.update_status(item, DownloadStatus.STARTED)
        if progress_monitor and worker_id is not None:
            progress_monitor.update_worker_status(worker_id, WorkerStatus.DOWNLOADING, filename, size_bytes)
        
        print(f"[{datetime.now()}] Starting download: {filename} ({format_size(size_bytes)})")
        
        # For large files, ensure credentials are valid before starting
        if size_bytes > 100 * 1024 * 1024:  # Files larger than 100MB
            if not ensure_valid_credentials(profile_name):
                error_msg = "Credential validation failed"
                if progress_monitor and worker_id:
                    progress_monitor.update_worker_status(worker_id, WorkerStatus.FAILED, error=error_msg)
                print(f"[{datetime.now()}] Could not validate credentials for large file download: {filename}")
                manifest_manager.update_status(item, DownloadStatus.FAILED, error_msg)
                return False
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    if progress_monitor and worker_id:
                        progress_monitor.update_worker_status(worker_id, WorkerStatus.RETRYING, filename, size_bytes)
                    print(f"[{datetime.now()}] Retry {attempt}/{max_retries} for {filename}")
                    time.sleep(5)
                
                # Create enhanced progress callback if monitor is available
                progress_callback = None
                if progress_monitor and worker_id and size_bytes > 1024 * 1024:  # For files > 1MB
                    progress_callback = EnhancedProgressCallback(filename, size_bytes, worker_id, progress_monitor)
                
                # Download the file using boto3
                success = download_file_boto3(
                    bucket_name=bucket,
                    key=source_key,
                    local_path=dest_file,
                    show_progress=(size_bytes > 10 * 1024 * 1024 and not progress_monitor),  # Show basic progress only if no monitor
                    profile_name=profile_name,
                    progress_callback=progress_callback
                )
                
                if success:
                    if progress_monitor and worker_id:
                        progress_monitor.update_worker_status(worker_id, WorkerStatus.COMPLETED)
                    print(f"[{datetime.now()}] Completed: {filename}")
                    manifest_manager.update_status(item, DownloadStatus.COMPLETED)
                    return True
                else:
                    raise Exception("Download failed")
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                error_msg = format_boto3_error(e)
                
                # Check if retryable error
                is_retryable = error_code in [
                    'RequestTimeout', 'ServiceUnavailable', 'SlowDown',
                    'InternalError', 'RequestTimeTooSkewed', 'SignatureDoesNotMatch'
                ]
                
                if attempt < max_retries and is_retryable:
                    wait_time = min(30, 5 * (2 ** attempt))  # Exponential backoff
                    print(f"[{datetime.now()}] Retryable error for {filename}: {error_msg}")
                    print(f"  Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                    time.sleep(wait_time)
                    continue
                else:
                    error_msg = format_boto3_error(e)
                    if progress_monitor and worker_id:
                        progress_monitor.update_worker_status(worker_id, WorkerStatus.FAILED, error=error_msg)
                    print(f"[{datetime.now()}] Failed to download {filename}: {error_msg}")
                    manifest_manager.update_status(item, DownloadStatus.FAILED, error_msg)
                    return False
                    
            except Exception as e:
                error_msg = str(e)
                
                # Check if retryable error based on message
                is_retryable = any(pattern in error_msg.lower() for pattern in [
                    'connection', 'timeout', 'network', 'ssl', 'certificate'
                ])
                
                if attempt < max_retries and is_retryable:
                    wait_time = min(30, 5 * (2 ** attempt))
                    print(f"[{datetime.now()}] Connection error for {filename}: {error_msg}")
                    print(f"  Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                    time.sleep(wait_time)
                    continue
                else:
                    if progress_monitor and worker_id:
                        progress_monitor.update_worker_status(worker_id, WorkerStatus.FAILED, error=error_msg)
                    print(f"[{datetime.now()}] Failed to download {filename}: {error_msg}")
                    manifest_manager.update_status(item, DownloadStatus.FAILED, error_msg)
                    return False
        
        return False
        
    finally:
        # Always remove lock file and reset worker status
        if progress_monitor and worker_id:
            progress_monitor.update_worker_status(worker_id, WorkerStatus.IDLE)
        remove_lock_file(lock_file)

def download_worker(work_queue, base_dest_path, manifest_manager, max_retries, worker_id, profile_name, session_lock, progress_monitor=None):
    """Worker function for downloading files using boto3"""
    print(f"[{datetime.now()}] Worker {worker_id+1} started")
    
    if progress_monitor:
        progress_monitor.update_worker_status(worker_id, WorkerStatus.IDLE)
    
    while True:
        try:
            item = work_queue.get(timeout=5)
            if item is None:  # Shutdown signal
                break
            
            # Ensure valid session before download (with lock to coordinate across workers)
            with session_lock:
                if not ensure_valid_credentials(profile_name):
                    print(f"[{datetime.now()}] Worker {worker_id+1}: Could not validate credentials")
                    work_queue.put(item)  # Put item back
                    break
            
            # Download the file
            success = download_single_file(item, base_dest_path, manifest_manager, max_retries, profile_name, progress_monitor, worker_id)
            
            work_queue.task_done()
            
        except queue.Empty:
            if progress_monitor:
                progress_monitor.update_worker_status(worker_id, WorkerStatus.IDLE)
            break
        except Exception as e:
            if progress_monitor:
                progress_monitor.update_worker_status(worker_id, WorkerStatus.FAILED, error=str(e))
            print(f"[{datetime.now()}] Worker {worker_id+1} error: {e}")
            work_queue.task_done()
    
    if progress_monitor:
        progress_monitor.update_worker_status(worker_id, WorkerStatus.IDLE)
    print(f"[{datetime.now()}] Worker {worker_id+1} finished")

def main():
    args = parse_arguments()
    base_dest_path = os.path.abspath(args.destination)
    profile_name = args.profile
    
    # Load buckets
    buckets = load_buckets_from_file(args.buckets_file)
    if not buckets:
        return
    
    print(f"Loaded {len(buckets)} buckets: {', '.join(buckets)}")
    print(f"Destination: {base_dest_path}")
    print(f"AWS Profile: {profile_name}")
    
    # Check AWS credentials
    if not ensure_valid_credentials(profile_name):
        print("Failed to authenticate. Please run 'aws sso login --profile " + profile_name + "' and try again.")
        return
    
    # Initialize manifest manager
    manifest_manager = ManifestManager(args.manifest)
    
    if args.generate_manifest or not os.path.exists(args.manifest):
        # Generate manifest
        total_items, total_size = generate_manifest(buckets, manifest_manager, profile_name)
        
        if args.generate_manifest:
            print(f"\nManifest saved to: {args.manifest}")
            return
    
    # Get pending items (and failed items if requested)
    pending_items = manifest_manager.get_pending_items(include_failed=args.retry_failed)
    
    if not pending_items:
        print("No pending downloads found.")
        if not args.retry_failed:
            print("Use --retry-failed to include failed items for retry.")
        return
    
    # Show statistics
    stats, total_size, completed_size = manifest_manager.get_stats()
    print(f"\nDownload Statistics:")
    print(f"  Pending: {stats['pending']}")
    print(f"  Completed: {stats['completed']}")
    print(f"  Failed: {stats['failed']}")
    if args.retry_failed and stats['failed'] > 0:
        print(f"  → Including {stats['failed']} failed items for retry")
    print(f"  Total size: {format_size(total_size)}")
    print(f"  Completed size: {format_size(completed_size)}")
    print(f"  Remaining: {format_size(total_size - completed_size)}")
    
    if args.dry_run:
        retry_msg = " (including failed items)" if args.retry_failed else ""
        print(f"\n[DRY-RUN] Would download {len(pending_items)} files{retry_msg} with {args.max_workers} workers")
        return
    
    # Create destination directory
    os.makedirs(base_dest_path, exist_ok=True)
    
    # Initialize progress monitor
    progress_monitor = ProgressMonitor(args.max_workers)
    
    # Update initial stats
    progress_monitor.update_overall_stats(
        total_files=stats['pending'] + stats['completed'] + stats['failed'],
        completed_files=stats['completed'],
        failed_files=stats['failed'],
        pending_files=len(pending_items),
        total_size=total_size,
        downloaded_size=completed_size
    )
    
    # Start multi-threaded downloads
    print(f"\n[{datetime.now()}] Starting downloads with {args.max_workers} workers...")
    
    # Start progress monitoring
    progress_monitor.start()
    
    work_queue = queue.Queue()
    session_lock = threading.Lock()  # Coordinate credential checks across workers
    
    # Add pending items to queue
    for item in pending_items:
        work_queue.put(item)
    
    # Start workers
    workers = []
    for i in range(args.max_workers):
        worker = threading.Thread(
            target=download_worker,
            args=(work_queue, base_dest_path, manifest_manager, args.max_retries, i, profile_name, session_lock, progress_monitor)
        )
        worker.start()
        workers.append(worker)
    
    start_time = datetime.now()
    
    try:
        # Monitor progress with more frequent updates
        while not work_queue.empty() or any(worker.is_alive() for worker in workers):
            # Update overall statistics
            stats, total_size, completed_size = manifest_manager.get_stats()
            progress_monitor.update_overall_stats(
                completed_files=stats['completed'],
                failed_files=stats['failed'],
                pending_files=stats['pending'],
                downloaded_size=completed_size
            )
            
            # Force refresh the display
            progress_monitor.refresh()
            
            try:
                time.sleep(1)  # Update every 1 second for better responsiveness
            except KeyboardInterrupt:
                break
        
        # Wait for all workers to complete
        work_queue.join()
        
        # Signal workers to stop
        for _ in workers:
            work_queue.put(None)
        
        for worker in workers:
            worker.join()
        
    except KeyboardInterrupt:
        print("\nInterrupted by user. Waiting for current downloads to complete...")
        
        # Signal workers to stop
        for _ in workers:
            work_queue.put(None)
        
        for worker in workers:
            worker.join(timeout=30)
    
    finally:
        # Stop progress monitoring
        progress_monitor.stop()
    
    # Final statistics
    stats, total_size, completed_size = manifest_manager.get_stats()
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print(f"\n[{datetime.now()}] Download session completed!")
    print(f"Final Statistics:")
    print(f"  Completed: {stats['completed']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Pending: {stats['pending']}")
    print(f"  Total downloaded: {format_size(completed_size)}")
    print(f"  Total time: {elapsed/3600:.1f} hours")
    if completed_size > 0:
        print(f"  Average rate: {format_size(completed_size/elapsed)}/s")

if __name__ == "__main__":
    main()
