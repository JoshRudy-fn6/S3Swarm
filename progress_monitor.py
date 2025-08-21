"""
Enhanced Progress Monitor for S3Swarm

Provides a real-time, non-scrolling dashboard showing:
- Individual worker progress
- Overall statistics
- Download speeds and ETAs
"""

import threading
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, Optional, List
from enum import Enum

try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.progress import Progress, ProgressColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, SpinnerColumn
    from rich.panel import Panel
    from rich.columns import Columns
    from rich.layout import Layout
    from rich.text import Text
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

class WorkerStatus(Enum):
    IDLE = "idle"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"

@dataclass
class WorkerInfo:
    id: int
    status: WorkerStatus = WorkerStatus.IDLE
    current_file: str = ""
    file_size: int = 0
    bytes_downloaded: int = 0
    download_speed: float = 0.0
    start_time: Optional[datetime] = None
    total_completed: int = 0
    total_failed: int = 0
    retry_count: int = 0
    current_error: str = ""

@dataclass
class OverallStats:
    total_files: int = 0
    completed_files: int = 0
    failed_files: int = 0
    total_size: int = 0
    downloaded_size: int = 0
    active_workers: int = 0
    total_retries: int = 0
    start_time: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None

class ProgressMonitor:
    """Enhanced progress monitor with rich dashboard optimized for high worker counts"""
    
    def __init__(self, max_workers: int = 30):  # Updated default for high-bandwidth optimization
        self.max_workers = max_workers
        self.workers: Dict[int, WorkerInfo] = {}
        self.overall_stats = OverallStats()
        self.lock = threading.Lock()
        self.console = Console() if RICH_AVAILABLE else None
        self.live: Optional[Live] = None
        self.is_running = False
        
        # Initialize workers
        for i in range(max_workers):
            self.workers[i] = WorkerInfo(id=i)
    
    def start(self):
        """Start the progress monitor"""
        if not RICH_AVAILABLE:
            return
        
        self.is_running = True
        self.overall_stats.start_time = datetime.now()
        
        if self.live is None:
            self.live = Live(self._create_layout(), refresh_per_second=4, console=self.console)
            self.live.start()
    
    def stop(self):
        """Stop the progress monitor"""
        self.is_running = False
        if self.live:
            self.live.stop()
            self.live = None
    
    def set_total_files(self, total_files: int, total_size: int):
        """Set the total number of files and size to download"""
        with self.lock:
            self.overall_stats.total_files = total_files
            self.overall_stats.total_size = total_size
    
    def update_worker_status(self, worker_id: int, status: WorkerStatus, 
                           current_file: str = "", file_size: int = 0,
                           bytes_downloaded: int = 0, error: str = ""):
        """Update worker status"""
        with self.lock:
            if worker_id in self.workers:
                worker = self.workers[worker_id]
                old_status = worker.status
                
                # If completing a download, subtract the in-progress bytes from overall stats
                # since they'll be added to completed_size separately
                if (old_status == WorkerStatus.DOWNLOADING and 
                    status == WorkerStatus.COMPLETED and 
                    worker.bytes_downloaded > 0):
                    self.overall_stats.downloaded_size -= worker.bytes_downloaded
                
                worker.status = status
                worker.current_file = current_file
                worker.file_size = file_size
                worker.bytes_downloaded = bytes_downloaded
                worker.current_error = error
                
                # Handle status transitions
                if status == WorkerStatus.DOWNLOADING and old_status != WorkerStatus.DOWNLOADING:
                    worker.start_time = datetime.now()
                    worker.retry_count = 0
                elif status == WorkerStatus.RETRYING:
                    worker.retry_count += 1
                    self.overall_stats.total_retries += 1
                elif status == WorkerStatus.COMPLETED:
                    worker.total_completed += 1
                    worker.current_file = ""
                    worker.bytes_downloaded = 0
                    worker.start_time = None
                elif status == WorkerStatus.FAILED:
                    worker.total_failed += 1
                    worker.current_file = ""
                    worker.bytes_downloaded = 0
                    worker.start_time = None
        
        # Force layout update
        if self.live and self.is_running:
            self.live.update(self._create_layout())
    
    def update_worker_progress(self, worker_id: int, bytes_downloaded: int):
        """Update worker download progress"""
        with self.lock:
            if worker_id in self.workers:
                worker = self.workers[worker_id]
                old_bytes = worker.bytes_downloaded
                worker.bytes_downloaded = bytes_downloaded
                
                # Update overall downloaded size with the delta
                bytes_delta = bytes_downloaded - old_bytes
                if bytes_delta > 0:
                    self.overall_stats.downloaded_size += bytes_delta
                
                # Calculate download speed
                if worker.start_time and bytes_downloaded > 0:
                    elapsed = (datetime.now() - worker.start_time).total_seconds()
                    if elapsed > 0:
                        worker.download_speed = bytes_downloaded / elapsed
        
        # Don't force layout update for every progress change to avoid performance issues
        # The refresh() method will be called by the main loop
    
    def file_completed(self, worker_id: int, file_size: int):
        """Mark a file as completed"""
        with self.lock:
            self.overall_stats.completed_files += 1
            self.overall_stats.downloaded_size += file_size
            
            # Update ETA
            self._update_eta()
    
    def file_failed(self, worker_id: int):
        """Mark a file as failed"""
        with self.lock:
            self.overall_stats.failed_files += 1
    
    def update_overall_stats(self, completed_files: int = None, failed_files: int = None, 
                           pending_files: int = None, downloaded_size: int = None,
                           total_files: int = None, total_size: int = None):
        """Update overall statistics"""
        with self.lock:
            if completed_files is not None:
                self.overall_stats.completed_files = completed_files
            if failed_files is not None:
                self.overall_stats.failed_files = failed_files
            if downloaded_size is not None:
                self.overall_stats.downloaded_size = downloaded_size
            if total_files is not None:
                self.overall_stats.total_files = total_files
            elif pending_files is not None:
                # Calculate total_files if not provided directly
                self.overall_stats.total_files = (
                    self.overall_stats.completed_files + 
                    self.overall_stats.failed_files + 
                    pending_files
                )
            if total_size is not None:
                self.overall_stats.total_size = total_size
            
            # Update ETA
            self._update_eta()
    
    def _update_eta(self):
        """Update estimated completion time"""
        if not self.overall_stats.start_time:
            return
        
        elapsed = (datetime.now() - self.overall_stats.start_time).total_seconds()
        if elapsed <= 0 or self.overall_stats.completed_files <= 0:
            return
        
        # Calculate rate based on completed files
        files_per_second = self.overall_stats.completed_files / elapsed
        remaining_files = self.overall_stats.total_files - self.overall_stats.completed_files
        
        if files_per_second > 0:
            eta_seconds = remaining_files / files_per_second
            self.overall_stats.estimated_completion = datetime.now() + timedelta(seconds=eta_seconds)
    
    def _create_layout(self) -> Panel:
        """Create the rich layout"""
        if not RICH_AVAILABLE:
            return Panel("Rich library not available")
        
        # Worker status table
        worker_table = self._create_worker_table()
        
        # Overall statistics
        stats_panel = self._create_stats_panel()
        
        # Combine into layout
        layout = Columns([worker_table, stats_panel], equal=True, expand=True)
        
        return Panel(layout, title="[bold cyan]S3Swarm Download Progress", border_style="blue")
    
    def _create_worker_table(self) -> Table:
        """Create worker status table"""
        table = Table(title="Worker Status", show_header=True, header_style="bold magenta")
        table.add_column("Worker", style="cyan", width=8)
        table.add_column("Status", style="green", width=12)
        table.add_column("Current File", style="yellow", width=30)
        table.add_column("Progress", style="blue", width=20)
        table.add_column("Speed", style="magenta", width=12)
        
        with self.lock:
            active_workers = 0
            for worker in self.workers.values():
                # Status with color
                status_color = {
                    WorkerStatus.IDLE: "white",
                    WorkerStatus.DOWNLOADING: "green",
                    WorkerStatus.COMPLETED: "blue",
                    WorkerStatus.FAILED: "red",
                    WorkerStatus.RETRYING: "yellow"
                }.get(worker.status, "white")
                
                status_text = f"[{status_color}]{worker.status.value.title()}[/{status_color}]"
                
                # Current file (truncated)
                current_file = worker.current_file
                if len(current_file) > 25:
                    current_file = "..." + current_file[-22:]
                
                # Progress bar
                if worker.file_size > 0:
                    progress = (worker.bytes_downloaded / worker.file_size) * 100
                    progress_bar = f"{progress:.1f}% ({self._format_size(worker.bytes_downloaded)}/{self._format_size(worker.file_size)})"
                else:
                    progress_bar = "N/A"
                
                # Speed
                speed_text = f"{self._format_size(worker.download_speed)}/s" if worker.download_speed > 0 else "N/A"
                
                table.add_row(
                    f"#{worker.id}",
                    status_text,
                    current_file,
                    progress_bar,
                    speed_text
                )
                
                if worker.status in [WorkerStatus.DOWNLOADING, WorkerStatus.RETRYING]:
                    active_workers += 1
            
            self.overall_stats.active_workers = active_workers
        
        return table
    
    def _create_stats_panel(self) -> Panel:
        """Create overall statistics panel"""
        with self.lock:
            stats = self.overall_stats
            
            # Calculate total downloaded including active download progress
            total_downloaded = stats.downloaded_size
            for worker in self.workers.values():
                if worker.status == WorkerStatus.DOWNLOADING:
                    total_downloaded += worker.bytes_downloaded
            
            # Calculate percentages
            file_progress = (stats.completed_files / max(stats.total_files, 1)) * 100
            size_progress = (total_downloaded / max(stats.total_size, 1)) * 100
            
            # Calculate overall speed from all active workers
            overall_speed = 0
            if stats.start_time:
                elapsed = (datetime.now() - stats.start_time).total_seconds()
                if elapsed > 0:
                    # Use total downloaded including progress for speed calculation
                    overall_speed = total_downloaded / elapsed
                    
                    # Also add current worker speeds for real-time rate
                    current_worker_speed = sum(worker.download_speed for worker in self.workers.values() 
                                             if worker.status == WorkerStatus.DOWNLOADING)
                    
                    # Use the higher of average speed or current instantaneous speed
                    overall_speed = max(overall_speed, current_worker_speed)
            
            # ETA calculation based on remaining data and current speed
            eta_text = "Calculating..."
            if overall_speed > 0 and stats.total_size > total_downloaded:
                remaining_bytes = stats.total_size - total_downloaded
                eta_seconds = remaining_bytes / overall_speed
                eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                eta_text = eta_time.strftime("%H:%M:%S")
            elif stats.total_size <= total_downloaded:
                eta_text = "Complete"
            
            # Runtime
            runtime = "00:00:00"
            if stats.start_time:
                elapsed = datetime.now() - stats.start_time
                runtime = str(elapsed).split('.')[0]  # Remove microseconds
            
            stats_text = f"""[bold]Overall Progress[/bold]
Files: {stats.completed_files}/{stats.total_files} ({file_progress:.1f}%)
Size: {self._format_size(total_downloaded)}/{self._format_size(stats.total_size)} ({size_progress:.1f}%)

[bold]Performance[/bold]
Overall Speed: {self._format_size(overall_speed)}/s
Runtime: {runtime}
ETA: {eta_text}
Active Workers: {stats.active_workers}/{self.max_workers}

[bold]Summary[/bold]
Completed: {stats.completed_files}
Failed: {stats.failed_files}
Retries: {stats.total_retries}"""
        
        return Panel(stats_text, title="Statistics", border_style="green")
    
    def _format_size(self, size: float) -> str:
        """Format size in bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"
    
    def refresh(self):
        """Force a refresh of the display"""
        if self.live and self.is_running:
            self.live.update(self._create_layout())

# Enhanced progress callback for individual file downloads
class EnhancedProgressCallback:
    """Progress callback that updates the progress monitor"""
    
    def __init__(self, filename: str, file_size: int, worker_id: int, progress_monitor: ProgressMonitor):
        self.filename = filename
        self.file_size = file_size
        self.worker_id = worker_id
        self.progress_monitor = progress_monitor
        self.last_update = 0
        self.update_threshold = max(file_size // 1000, 64 * 1024)  # Update every 0.1% or 64KB for more frequent updates
        self.last_time = time.time()
    
    def __call__(self, bytes_transferred: int):
        """Called by boto3 during download"""
        # Update more frequently for better real-time tracking
        current_time = time.time()
        if (bytes_transferred - self.last_update >= self.update_threshold or 
            current_time - self.last_time >= 0.5):  # Update at least every 0.5 seconds
            self.progress_monitor.update_worker_progress(self.worker_id, bytes_transferred)
            self.last_update = bytes_transferred
            self.last_time = current_time
