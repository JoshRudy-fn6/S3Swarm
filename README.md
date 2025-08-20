# S3Swarm - Orchestrated S3 Data Collection

## 🐝 Why S3Swarm?

When faced with migrating terabytes of data from AWS S3, traditional single-threaded approaches can take weeks to complete. AWS throttles individual connections to 20-60 Mbps, making a 4.8TB dataset take over 12 days to download with conventional tools. The solution? Deploy a swarm.

S3Swarm takes inspiration from nature's most efficient workers - bees. Just as a bee colony deploys multiple workers to efficiently collect nectar from distributed flower beds, S3Swarm deploys multiple worker threads to harvest data from distributed S3 buckets. Each worker operates independently yet coordinates seamlessly with the hive, avoiding conflicts while maximizing throughput.

The result? What once took weeks now completes in days. Multiple concurrent connections multiply your effective bandwidth, while intelligent retry logic and progress tracking ensure no data is left behind. Like a well-organized hive, S3Swarm brings order to the chaos of large-scale data migration.

A robust, orchestrated Python tool for efficiently downloading large datasets from AWS S3 buckets with intelligent worker coordination, manifest tracking, and automatic recovery.

## 🚀 Features

- **Orchestrated worker swarm** - Deploy multiple workers that coordinate seamlessly
- **Intelligent progress tracking** - XML manifest system tracks every file
- **Automatic retry logic** - Handle connection failures gracefully  
- **Lock file protection** - Prevent worker conflicts in concurrent operations
- **Real-time monitoring** - Progress updates with ETA and download rates
- **SSO coordination** - Thread-safe AWS SSO credential management
- **Flexible bucket configuration** - Simple text file for target management
- **Safe operation** - Read-only S3 operations, no risk of data modification
- **Enhanced error handling** - SSL/connection errors with exponential backoff
- **SSO coordination** - Thread-safe SSO token renewal across workers
- **Input validation** - Filters out invalid entries and empty folders

## 📋 Prerequisites

- Python 3.7+
- AWS CLI installed and configured
- Valid AWS credentials with S3 read access
- Sufficient local storage space

## 🛠️ Installation

1. Clone this repository:
```bash
[git clone https://github.com/JoshRudy-fn6/S3Swarm.git
cd S3Swarm
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS CLI:
```bash
aws configure sso
```

## 📁 Project Structure

```
s3swarm/
├── s3swarm.py                  # Main orchestration script
├── buckets.txt                 # Target S3 buckets (flower beds)
├── download_manifest.xml       # Progress tracking (hive inventory)
├── requirements.txt            # Python dependencies
└── README.md                   # Beekeeper's guide
```

## 🔧 Configuration

### 1. Bucket Configuration

Edit `buckets.txt` to specify which S3 buckets to download:

```
# One bucket per line
my-company-backups
data-archive-bucket
production-exports

# Lines starting with # are comments
# my-other-bucket
```

### 2. AWS Profile

Update the profile name in `s3swarm.py` if needed:

```python
profile = "your-aws-profile-name"  # e.g., "production-profile"
```

## 🚀 Usage

### Step 1: Generate Manifest

First, generate a complete manifest of all files to be downloaded:

```bash
python s3swarm.py --generate-manifest
```

This will:
- Scan all specified S3 buckets
- Calculate total file sizes
- Create `download_manifest.xml` with all file details
- Provide total size estimate and file count

### Step 2: Start Downloads

Deploy your worker swarm to begin harvesting:

```bash
# Conservative swarm (4 workers)
python s3swarm.py --max-workers 4

# Specify custom destination
python s3swarm.py --destination /data/aws_backup --max-workers 6

# Aggressive swarm (8 workers)
python s3swarm.py --max-workers 8 --max-retries 5

# Retry failed downloads from previous session
python s3swarm.py --max-workers 6 --retry-failed

# Test run (no actual downloads)
python s3swarm.py --dry-run --max-workers 4
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--destination` | `./s3_downloads` | Directory where files will be downloaded |
| `--buckets-file` | `buckets.txt` | Text file containing bucket names |
| `--manifest` | `download_manifest.xml` | Manifest file path |
| `--max-workers` | `4` | Maximum concurrent downloads |
| `--max-retries` | `3` | Maximum retries per failed download |
| `--generate-manifest` | `False` | Only generate manifest, don't download |
| `--dry-run` | `False` | Show what would be downloaded |
| `--retry-failed` | `False` | Include failed items in download queue for retry |

## 📊 Example Output

### Manifest Generation
```
[2025-08-20 10:00:00] Generating manifest for 3 buckets...
[2025-08-20 10:00:05] Processing bucket: my-company-backups
  Found 21 items
  Analyzing folder: VM_Images
    Added 15 files (8.5 GB)
  Analyzing file: database_backup.zip
    Added 1 files (12.3 GB)

[2025-08-20 10:05:30] Manifest generation complete!
Total items: 2,847
Total size: 4.8 TB
```

### Download Progress
```
Loaded 3 buckets: my-company-backups, data-archive-bucket, production-exports
Destination: /data/aws_backup

[2025-08-20 10:10:00] Deploying swarm with 6 workers...
[2025-08-20 10:10:01] Worker 1 started
[2025-08-20 10:10:01] Worker 2 started
[2025-08-20 10:10:03] Worker 1: Starting download: VM_Image_01.vmdk (3.2 GB)
[2025-08-20 10:10:05] Worker 2: Starting download: backup_archive.zip (1.1 GB)
[2025-08-20 10:10:07] Worker 3: Starting download: system_image.ova (4.1 GB)

[2025-08-20 10:15:30] Swarm Progress: 45/2847 files, 78.3 GB/4.8 TB (1.6%)
Rate: 125.3 MB/s, ETA: 8.2h, Active Workers: 6/6

[2025-08-20 10:16:00] Worker 1: Completed: VM_Image_01.vmdk
[2025-08-20 10:16:05] Worker 2: SSL/Connection error for large_file.tar: UNEXPECTED_EOF_WHILE_READING
  Worker 2: Waiting 5s before retry 1/3...
[2025-08-20 10:16:15] Worker 2: Completed: large_file.tar
```

## 🔄 Resuming Downloads

The script automatically resumes interrupted downloads:

1. **Manifest tracks progress** - Files marked as `completed` are skipped
2. **Lock files prevent conflicts** - Active downloads are protected
3. **Perfect resume** - Simply rerun the same command
4. **Retry failed items** - Use `--retry-failed` to retry previously failed downloads

```bash
# Resume after interruption
python s3swarm.py --max-workers 6

# Retry failed downloads from previous session
python s3swarm.py --max-workers 6 --retry-failed
```

## 🛡️ Safety Features

- **Read-only operations** - Cannot modify or delete S3 data
- **Lock file protection** - Prevents concurrent download conflicts
- **Automatic retries** - Handles temporary connection issues
- **Progress persistence** - Resume from exactly where you left off
- **SSO management** - Automatic credential renewal

## 📈 Performance

### Swarm Performance Improvements

| Workers | Improvement | Use Case |
|---------|-------------|----------|
| 1 | Baseline | Single worker (traditional approach) |
| 4 | 3-4x faster | Conservative swarm, stable operations |
| 6-8 | 5-7x faster | Aggressive swarm, maximum throughput |
| 12+ | Variable | May hit bandwidth or AWS limits |

### AWS Connection Throttling

AWS typically throttles individual connections to 20-60 Mbps. S3Swarm deploys multiple workers to open concurrent connections, dramatically increasing total throughput - just like how a bee colony can collect far more nectar than a single bee.

**Real-world example**: 4.8 TB dataset migration
- Single connection: ~12 days
- 6-worker swarm: ~2-3 days (4x improvement)

## 🔧 Troubleshooting

### Common Issues

1. **SSO Token Expired**
   ```bash
   aws sso login --profile your-profile-name
   ```

2. **Permission Denied**
   - Verify S3 read permissions
   - Check bucket names in `buckets.txt`

3. **Lock Files Remaining**
   - Remove `*.lock` files from destination folders
   - Usually indicates interrupted downloads

4. **Manifest Corruption**
   - Delete `download_manifest.xml`
   - Regenerate with `--generate-manifest`

5. **SSL/Connection Errors**
   - Script automatically retries with exponential backoff
   - Common with large files or poor connections
   - Workers coordinate to prevent SSO conflicts

6. **Empty Folder Entries**
   - S3Swarm now filters out invalid entries automatically
   - Regenerate manifest if you encounter path errors

### Swarm Coordination

S3Swarm's workers are designed to operate independently while maintaining perfect coordination:
- **Lock files prevent conflicts** - No two workers download the same file
- **SSO token sharing** - Workers pause during credential renewal to prevent auth conflicts  
- **Progress synchronization** - Real-time manifest updates across all workers
- **Intelligent error handling** - Failed tasks are redistributed to healthy workers

### Performance Tuning

- **Start with a small swarm** (4 workers) and scale up gradually
- **Monitor network utilization** - more workers ≠ always faster
- **Check disk I/O** - ensure destination drive can handle swarm throughput
- **AWS account limits** - Some accounts have transfer rate restrictions
- **Worker coordination** - S3Swarm automatically manages SSO token renewal across the swarm

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

S3Swarm is designed for legitimate data migration and backup purposes. Users are responsible for:

- Ensuring proper permissions to access target S3 buckets
- Complying with organizational data handling policies  
- Respecting AWS usage terms and service conditions
- Managing AWS costs (data transfer fees apply)
- Using appropriate swarm sizes for their network infrastructure

## 🙏 Acknowledgments

- Inspired by the efficiency of bee colonies and swarm intelligence
- Built to solve real-world large-scale data migration challenges
- Designed for organizations moving terabytes of data from AWS S3
- Optimized for environments with limited backup infrastructure

---

**🐝 Deploy your swarm and harvest your data efficiently!**


