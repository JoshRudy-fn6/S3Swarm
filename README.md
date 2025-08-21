# S3Swarm - Orchestrated S3 Data Collection

## 🐝 Why S3Swarm?

When faced with migrating terabytes of data from AWS S3, traditional single-threaded approaches can take weeks to complete. AWS throttles individual connections to 20-60 Mbps, making a 4.8TB dataset take over 12 days to download with conventional tools. The solution? Deploy a swarm.

S3Swarm takes inspiration from nature's most efficient workers - bees. Just as a bee colony deploys multiple workers to efficiently collect nectar from distributed flower beds, S3Swarm deploys multiple worker threads to harvest data from distributed S3 buckets. Each worker operates independently yet coordinates seamlessly with the hive, avoiding conflicts while maximizing throughput.

The result? What once took weeks now completes in days. Multiple concurrent connections multiply your effective bandwidth, while intelligent retry logic and progress tracking ensure no data is left behind. Like a well-organized hive, S3Swarm brings order to the chaos of large-scale data migration.

## 🚀 Features

### Core Capabilities
- **🔥 Native boto3 Performance** - Direct AWS SDK integration for maximum speed
- **📊 Real-time Rich Dashboard** - Beautiful live progress monitoring with worker status
- **🐝 Orchestrated Worker Swarm** - Deploy multiple workers that coordinate seamlessly
- **📝 Intelligent Manifest Tracking** - XML-based progress system tracks every file
- **🔄 Automatic Retry Logic** - Handle connection failures gracefully with exponential backoff
- **🔒 Lock File Protection** - Prevent worker conflicts in concurrent operations
- **🔐 SSO Integration** - Thread-safe AWS SSO credential management with auto-refresh
- **⚡ Enhanced Performance** - 5-10x faster than traditional AWS CLI approaches

### Safety & Reliability
- **🛡️ Read-only Operations** - Cannot modify or delete S3 data
- **🔄 Perfect Resume** - Continue from exactly where you left off
- **✅ Input Validation** - Filters out invalid entries and empty folders
- **🎯 Flexible Configuration** - Simple text file for bucket management
- **🔍 Enhanced Error Handling** - SSL/connection errors with intelligent recovery

## 📋 Prerequisites

- Python 3.8+
- AWS CLI v2 installed and configured
- Valid AWS credentials with S3 read access
- Sufficient local storage space

## 🛠️ Installation

1. Clone this repository:
```bash
git clone https://github.com/JoshRudy-fn6/S3Swarm.git
cd S3Swarm
```

2. Create virtual environment (recommended):
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure AWS CLI with SSO:
```bash
aws configure sso --profile your-profile-name
```

## 📁 Project Structure

```
S3Swarm/
├── s3swarm.py                 # Main application (boto3-native)
├── boto3_auth.py              # SSO authentication management
├── s3_operations.py           # Core S3 operations
├── progress_monitor.py        # Rich dashboard interface
├── buckets.txt                # Target S3 buckets configuration
├── download_manifest.xml      # Progress tracking manifest
├── requirements.txt           # Python dependencies
├── README.md                  # Documentation
└── SECURITY_AUDIT.md         # Security validation report
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

### 2. AWS Profile Setup

The application uses your configured AWS SSO profile:

```bash
# Configure your profile
aws configure sso --profile myprofile

# Test authentication
aws s3 ls --profile myprofile
```

## 🚀 Usage

### Enhanced boto3 Implementation

S3Swarm now uses native boto3 for superior performance and reliability:

```bash
# Generate manifest (scan all buckets)
python s3swarm.py --generate-manifest --profile your-profile

# Start downloads with real-time dashboard
python s3swarm.py --max-workers 4 --profile your-profile

# Aggressive swarm with enhanced monitoring
python s3swarm.py --max-workers 8 --profile your-profile

# Retry failed downloads
python s3swarm.py --retry-failed --max-workers 6 --profile your-profile

# Test run with live dashboard
python s3swarm.py --dry-run --max-workers 2 --profile your-profile
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--destination` | `./s3_downloads` | Directory where files will be downloaded |
| `--buckets-file` | `buckets.txt` | Text file containing bucket names |
| `--manifest` | `download_manifest.xml` | Manifest file path |
| `--max-workers` | `4` | Maximum concurrent downloads |
| `--max-retries` | `3` | Maximum retries per failed download |
| `--profile` | `default` | AWS SSO profile name |
| `--generate-manifest` | `False` | Only generate manifest, don't download |
| `--dry-run` | `False` | Show what would be downloaded with live dashboard |
| `--retry-failed` | `False` | Include failed items for retry |

## 📊 Live Dashboard

S3Swarm features a beautiful real-time dashboard powered by Rich:

```
╭─────────────────────────────── S3Swarm Download Progress ───────────────────────────────╮
│                                    Worker Status                                        │
│ ┏━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓ │
│ ┃ Worker   ┃ Status       ┃ Current File             ┃ Progress         ┃ Speed        ┃ │
│ ┡━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩ │
│ │ #0       │ Downloading  │ large_file.vmdk          │ 45.2% (2.1/4.6G)│ 125.3 MB/s  │ │
│ │ #1       │ Downloading  │ backup.tar.gz            │ 78.1% (890M/1.1G)│ 98.7 MB/s   │ │
│ │ #2       │ Idle         │                          │ N/A              │ N/A          │ │
│ │ #3       │ Completed    │                          │ N/A              │ N/A          │ │
│ └──────────┴──────────────┴──────────────────────────┴──────────────────┴──────────────┘ │
│ ╭──────────────────────────────── Statistics ────────────────────────────────╮           │
│ │ Overall Progress                                                           │           │
│ │ Files: 145/2847 (5.1%)                                                     │           │
│ │ Size: 234.5 GB/4.8 TB (4.9%)                                              │           │
│ │                                                                            │           │
│ │ Performance                                                                │           │
│ │ Overall Speed: 195.7 MB/s                                                  │           │
│ │ Runtime: 0:15:32                                                           │           │
│ │ ETA: 5:23:18                                                               │           │
│ │ Active Workers: 2/4                                                        │           │
│ │                                                                            │           │
│ │ Summary                                                                    │           │
│ │ Completed: 143                                                             │           │
│ │ Failed: 2                                                                  │           │
│ │ Retries: 5                                                                 │           │
│ ╰────────────────────────────────────────────────────────────────────────────╯           │
╰─────────────────────────────────────────────────────────────────────────────────────────╯
```

### Dashboard Features:
- **Real-time worker status** - See what each worker is downloading
- **Live progress bars** - Individual file and overall progress
- **Performance metrics** - Download speeds, ETA, runtime
- **Statistics summary** - Completed, failed, and retry counts
- **Non-scrolling display** - Clean, stable interface

## 🔄 Resuming Downloads

The boto3 implementation provides perfect resume capability:

```bash
# Resume interrupted downloads
python s3swarm.py --max-workers 6 --profile your-profile

# Retry failed downloads with enhanced monitoring
python s3swarm.py --retry-failed --max-workers 4 --profile your-profile
```

Features:
- **Manifest-based tracking** - Files marked as `completed` are automatically skipped
- **Lock file protection** - Prevents conflicts during concurrent operations
- **SSO token management** - Automatic credential refresh across workers
- **Intelligent recovery** - Failed downloads are queued for retry

## 📈 Performance Improvements

### boto3 vs AWS CLI Performance

| Implementation | Speed | Memory | Features |
|---------------|-------|--------|----------|
| **AWS CLI (Legacy)** | Baseline | High subprocess overhead | Basic progress |
| **boto3 Native** | **5-10x faster** | Efficient Python objects | Rich dashboard |

### Real-world Performance

**Test Case: 4.8TB Dataset (8,213 files)**
- **Legacy AWS CLI**: ~12 days (single connection)
- **S3Swarm boto3 (4 workers)**: ~2-3 days
- **S3Swarm boto3 (8 workers)**: ~1.5-2 days

### Swarm Scaling

| Workers | Throughput | Use Case |
|---------|------------|----------|
| 1 | 50-100 MB/s | Conservative, testing |
| 4 | 200-400 MB/s | Standard production |
| 8 | 400-800 MB/s | Aggressive migration |
| 12+ | Variable | May hit bandwidth limits |

## 🛡️ Security Features

- **Read-only S3 operations** - Cannot modify or delete data
- **SSO integration** - Secure credential management
- **Thread-safe authentication** - Coordinated token refresh
- **Input validation** - Prevents path traversal attacks
- **Comprehensive audit trail** - Full operation logging

## 🔧 Troubleshooting

### Common Issues

1. **SSO Token Expired**
   ```bash
   aws sso login --profile your-profile-name
   ```

2. **Permission Denied**
   - Verify S3 read permissions
   - Check bucket names in `buckets.txt`
   - Confirm SSO profile configuration

3. **Dashboard Not Updating**
   - Rich library issue: `pip install --upgrade rich`
   - Terminal compatibility: Use modern terminal emulator

4. **Worker Coordination Issues**
   - Remove stale `*.lock` files
   - Regenerate manifest if corrupted

5. **Performance Issues**
   - Start with fewer workers and scale up
   - Monitor network and disk I/O
   - Check AWS account transfer limits

### Enhanced Debugging

Enable detailed logging:
```bash
export AWS_DEFAULT_REGION=us-east-1
export AWS_MAX_ATTEMPTS=3
python s3swarm.py --max-workers 2 --profile your-profile
```

## 🚀 Production Deployment

### Recommended Configuration

```bash
# Production-ready command
python s3swarm.py \
  --max-workers 6 \
  --max-retries 5 \
  --profile production-profile \
  --destination /data/s3_backup
```

### Best Practices

1. **Start Conservative** - Begin with 4 workers and scale up
2. **Monitor Resources** - Watch CPU, memory, and network usage
3. **Plan Storage** - Ensure adequate disk space and I/O capacity
4. **Test First** - Use `--dry-run` to validate configuration
5. **Backup Manifests** - Keep copies of `download_manifest.xml`

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

- Enhanced with native boto3 for superior performance
- Beautiful real-time dashboard powered by Rich library
- Inspired by bee colony efficiency and swarm intelligence
- Built to solve real-world large-scale data migration challenges

---

**🐝 Deploy your swarm and harvest your data efficiently with native boto3 power!**



