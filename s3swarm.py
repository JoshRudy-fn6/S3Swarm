import os
import subprocess
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
    parser = argparse.ArgumentParser(description='S3Swarm - Orchestrated S3 data collection with worker swarm')
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

def check_sso_login():
    """Check if AWS SSO is still valid"""
    try:
        subprocess.run([
            "aws", "sts", "get-caller-identity", 
            "--profile", profile
        ], capture_output=True, text=True, check=True)
        return True
    except subprocess.CalledProcessError:
        return False

def renew_sso_login():
    """Renew AWS SSO login"""
    print(f"[{datetime.now()}] AWS SSO token expired. Please re-authenticate...")
    try:
        subprocess.run([
            "aws", "sso", "login", 
            "--profile", profile
        ], check=True)
        print(f"[{datetime.now()}] SSO login successful")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[{datetime.now()}] SSO login failed: {e}")
        return False

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

def parse_aws_size(size_str):
    """Parse AWS size string to bytes"""
    size_str = size_str.strip()
    if not size_str or size_str == '0' or size_str == '--':
        return 0
    
    # Remove commas and split size and unit
    size_str = size_str.replace(',', '')
    
    # Use regex to separate number and unit
    match = re.match(r'([\d.]+)\s*([A-Za-z]*)', size_str)
    if not match:
        return 0
    
    try:
        size_value = float(match.group(1))
        unit = match.group(2).upper() if match.group(2) else 'B'
        
        multipliers = {
            '': 1, 'B': 1, 'BYTE': 1, 'BYTES': 1,
            'KB': 1024, 'KILOBYTE': 1024, 'KILOBYTES': 1024,
            'MB': 1024**2, 'MEGABYTE': 1024**2, 'MEGABYTES': 1024**2,
            'GB': 1024**3, 'GIGABYTE': 1024**3, 'GIGABYTES': 1024**3,
            'TB': 1024**4, 'TERABYTE': 1024**4, 'TERABYTES': 1024**4
        }
        
        return int(size_value * multipliers.get(unit, 1))
    except (ValueError, KeyError):
        return 0

def list_bucket_contents(bucket_name):
    """List all items in a bucket (both files and folders)"""
    try:
        cmd = [
            "aws", "s3", "ls", f"s3://{bucket_name}/", 
            "--profile", profile
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        items = []
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
            
            parts = line.split()
            if len(parts) >= 2:
                if line.endswith('/'):  # Directory
                    folder_name = parts[-1].rstrip('/')
                    items.append((folder_name, 'folder'))
                else:  # File
                    if len(parts) >= 3:
                        filename = ' '.join(parts[3:])  # Handle filenames with spaces
                        items.append((filename, 'file'))
        
        return items
    except subprocess.CalledProcessError as e:
        print(f"Error listing bucket {bucket_name}: {e}")
        return []

def get_detailed_file_listing(bucket_name, item_name, item_type):
    """Get detailed file listing for an item (file or folder)"""
    files = []
    
    try:
        if item_type == 'file':
            # Single file
            cmd = [
                "aws", "s3", "ls", f"s3://{bucket_name}/{item_name}",
                "--profile", profile
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            line = result.stdout.strip()
            
            if line:
                parts = line.split()
                if len(parts) >= 3:
                    size_bytes = int(parts[2])
                    files.append({
                        'filename': item_name,
                        'size': size_bytes,
                        'path': item_name
                    })
        
        else:  # folder
            # Recursive folder listing
            cmd = [
                "aws", "s3", "ls", f"s3://{bucket_name}/{item_name}/", 
                "--recursive", "--profile", profile
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            for line in result.stdout.strip().split('\n'):
                line = line.strip()
                if not line or line.startswith('PRE'):
                    continue
                
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        size_bytes = int(parts[2])
                        file_path = ' '.join(parts[3:])
                        filename = os.path.basename(file_path)
                        
                        # Skip empty filenames or directories
                        if not filename or not filename.strip():
                            continue
                        
                        files.append({
                            'filename': filename,
                            'size': size_bytes,
                            'path': file_path
                        })
                    except (ValueError, IndexError):
                        continue
    
    except subprocess.CalledProcessError as e:
        print(f"Error getting detailed listing for {bucket_name}/{item_name}: {e}")
    
    return files

def generate_manifest(buckets, manifest_manager):
    """Generate complete manifest of all files to download"""
    print(f"[{datetime.now()}] Generating manifest for {len(buckets)} buckets...")
    
    total_items = 0
    total_size = 0
    
    for bucket in buckets:
        print(f"\n[{datetime.now()}] Processing bucket: {bucket}")
        
        # Get bucket contents
        items = list_bucket_contents(bucket)
        print(f"  Found {len(items)} items")
        
        for item_name, item_type in items:
            print(f"  Analyzing {item_type}: {item_name}")
            
            # Get detailed file listing
            files = get_detailed_file_listing(bucket, item_name, item_type)
            
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

def download_single_file(item, base_dest_path, manifest_manager, max_retries=3):
    """Download a single file with lock file protection"""
    bucket = item.get("bucket")
    folder = item.get("folder")
    filename = item.get("filename")
    file_path = item.get("file_path")
    size_bytes = int(item.get("size", 0))
    
    # Skip invalid entries (empty filenames, directories, etc.)
    if not filename or not filename.strip():
        print(f"[{datetime.now()}] Skipping invalid entry with empty filename in {bucket}/{folder}")
        manifest_manager.update_status(item, DownloadStatus.FAILED, "Invalid entry - empty filename")
        return False
    
    # Determine source and destination paths
    if file_path and file_path != filename:
        # File is in a subfolder structure
        source_path = f"s3://{bucket}/{file_path}"
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
        source_path = f"s3://{bucket}/{folder}"
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
        print(f"[{datetime.now()}] Skipping {filename} - lock file exists")
        return False
    
    # Create lock file
    lock_file = create_lock_file(dest_dir, filename)
    if not lock_file:
        print(f"[{datetime.now()}] Could not create lock file for {filename}")
        return False
    
    try:
        # Update status to started
        manifest_manager.update_status(item, DownloadStatus.STARTED)
        
        print(f"[{datetime.now()}] Starting download: {filename} ({format_size(size_bytes)})")
        
        # For large files, check SSO token before starting
        if size_bytes > 100 * 1024 * 1024:  # Files larger than 100MB
            if not check_sso_login():
                print(f"[{datetime.now()}] SSO token expired before large file download, renewing...")
                if not renew_sso_login():
                    print(f"[{datetime.now()}] SSO renewal failed for {filename}")
                    manifest_manager.update_status(item, DownloadStatus.FAILED, "SSO renewal failed")
                    return False
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    print(f"[{datetime.now()}] Retry {attempt}/{max_retries} for {filename}")
                    time.sleep(5)
                
                # Download the file
                cmd = [
                    "aws", "s3", "cp", source_path, dest_file,
                    "--profile", profile
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                # Success
                print(f"[{datetime.now()}] Completed: {filename}")
                manifest_manager.update_status(item, DownloadStatus.COMPLETED)
                return True
                
            except subprocess.CalledProcessError as e:
                error_msg = e.stderr.strip() if e.stderr else str(e)
                
                # Check if retryable error
                is_retryable = any(pattern in error_msg.lower() for pattern in [
                    'connection broken', 'connection reset', 'connection closed',
                    'timeout', 'timed out', 'network error', 'ssl error',
                    'ssl validation failed', 'unexpected_eof_while_reading',
                    'eof occurred in violation of protocol', 'ssl:', 'certificate',
                    'handshake', 'throttling', 'rate limit'
                ])
                
                if attempt < max_retries and is_retryable:
                    # Use exponential backoff for SSL/connection errors
                    if any(ssl_pattern in error_msg.lower() for ssl_pattern in [
                        'ssl', 'unexpected_eof', 'eof occurred', 'handshake'
                    ]):
                        wait_time = min(30, 5 * (2 ** attempt))  # 5s, 10s, 20s, max 30s
                        print(f"[{datetime.now()}] SSL/Connection error for {filename}: {error_msg}")
                        print(f"  Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                        time.sleep(wait_time)
                    else:
                        print(f"[{datetime.now()}] Retryable error for {filename}: {error_msg}")
                        time.sleep(5)
                    continue
                else:
                    print(f"[{datetime.now()}] Failed to download {filename}: {error_msg}")
                    manifest_manager.update_status(item, DownloadStatus.FAILED, error_msg)
                    return False
        
        return False
        
    finally:
        # Always remove lock file
        remove_lock_file(lock_file)

def download_worker(work_queue, base_dest_path, manifest_manager, max_retries, worker_id, sso_lock):
    """Worker function for downloading files"""
    print(f"[{datetime.now()}] Worker {worker_id} started")
    
    while True:
        try:
            item = work_queue.get(timeout=5)
            if item is None:  # Shutdown signal
                break
            
            # Wait if another worker is renewing SSO
            with sso_lock:
                # Check SSO before download
                if not check_sso_login():
                    print(f"[{datetime.now()}] Worker {worker_id}: SSO expired, renewing...")
                    print(f"[{datetime.now()}] All workers paused during SSO renewal...")
                    if not renew_sso_login():
                        print(f"[{datetime.now()}] Worker {worker_id}: SSO renewal failed")
                        work_queue.put(item)  # Put item back
                        break
                    print(f"[{datetime.now()}] SSO renewed, workers resuming...")
            
            # Download the file
            success = download_single_file(item, base_dest_path, manifest_manager, max_retries)
            
            work_queue.task_done()
            
        except queue.Empty:
            break
        except Exception as e:
            print(f"[{datetime.now()}] Worker {worker_id} error: {e}")
            work_queue.task_done()
    
    print(f"[{datetime.now()}] Worker {worker_id} finished")

def main():
    args = parse_arguments()
    base_dest_path = os.path.abspath(args.destination)
    
    # Load buckets
    buckets = load_buckets_from_file(args.buckets_file)
    if not buckets:
        return
    
    print(f"Loaded {len(buckets)} buckets: {', '.join(buckets)}")
    print(f"Destination: {base_dest_path}")
    
    # Initialize manifest manager
    manifest_manager = ManifestManager(args.manifest)
    
    if args.generate_manifest or not os.path.exists(args.manifest):
        # Generate manifest
        total_items, total_size = generate_manifest(buckets, manifest_manager)
        
        if args.generate_manifest:
            print(f"\nManifest saved to: {args.manifest}")
            return
    
    # Check SSO
    if not check_sso_login():
        if not renew_sso_login():
            print("Failed to authenticate. Exiting.")
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
    
    # Start multi-threaded downloads
    print(f"\n[{datetime.now()}] Starting downloads with {args.max_workers} workers...")
    
    work_queue = queue.Queue()
    sso_lock = threading.Lock()  # Coordinate SSO renewals across workers
    
    # Add pending items to queue
    for item in pending_items:
        work_queue.put(item)
    
    # Start workers
    workers = []
    for i in range(args.max_workers):
        worker = threading.Thread(
            target=download_worker,
            args=(work_queue, base_dest_path, manifest_manager, args.max_retries, i+1, sso_lock)
        )
        worker.start()
        workers.append(worker)
    
    start_time = datetime.now()
    
    try:
        # Monitor progress
        while not work_queue.empty():
            stats, total_size, completed_size = manifest_manager.get_stats()
            elapsed = (datetime.now() - start_time).total_seconds()
            
            if completed_size > 0 and elapsed > 0:
                rate = completed_size / elapsed
                remaining = total_size - completed_size
                eta = remaining / rate if rate > 0 else 0
                
                print(f"[{datetime.now()}] Progress: {stats['completed']}/{stats['pending'] + stats['completed']} files, "
                      f"{format_size(completed_size)}/{format_size(total_size)} "
                      f"({completed_size/total_size*100:.1f}%), "
                      f"Rate: {format_size(rate)}/s, "
                      f"ETA: {eta/3600:.1f}h")
            
            time.sleep(30)  # Update every 30 seconds
        
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
