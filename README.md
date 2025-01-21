# Real Estate Data Scraper 🏠

A high-performance Python web scraper for collecting and analyzing real estate listings from major Azerbaijani property websites. Built with asyncio for maximum efficiency and scalability.

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Scraping](https://github.com/Ismat-Samadov/real_estate/actions/workflows/scraper.yaml/badge.svg)](https://github.com/Ismat-Samadov/real_estate/actions/workflows/scraper.yaml)

## 🎯 Supported Websites

| Website | Status | Features |
|---------|--------|-----------|
| [bina.az](https://bina.az) | ✅ Active | Full listing data, photos, contact info |
| [yeniemlak.az](https://yeniemlak.az) | ✅ Active | Full listing data, location info |
| [emlak.az](https://emlak.az) | ✅ Active | Full listing data, contact info |
| [lalafo.az](https://lalafo.az) | ✅ Active | API integration, full data |
| [tap.az](https://tap.az) | ✅ Active | Full listing data, photos |
| [ev10.az](https://ev10.az) | ✅ Active | Full listing data |
| [arenda.az](https://arenda.az) | ✅ Active | Full listing data, location info |
| [ipoteka.az](https://ipoteka.az) | ✅ Active | Full listing data, mortgage info |
| [unvan.az](https://unvan.az) | ✅ Active | Full listing data |
| [vipemlak.az](https://vipemlak.az) | ✅ Active | Full listing data |

## 🚀 Deployment Guide

### Server Requirements

#### Hardware
- CPU: 2+ cores recommended
- RAM: 2GB minimum, 4GB recommended
- Storage: 20GB minimum for database growth
- Network: Stable internet connection with good bandwidth

#### Software
- Ubuntu 20.04 LTS or newer
- Python 3.10+
- MySQL 8.0+
- Nginx (optional, for API deployment)
- Git
- SSL/TLS certificates

### Step-by-Step Deployment

#### 1. Server Setup
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y python3.10 python3.10-venv python3-pip mysql-server nginx git

# Install SSL certificates
sudo apt install -y certbot python3-certbot-nginx
```

#### 2. Database Setup
```bash
# Secure MySQL installation
sudo mysql_secure_installation

# Create database and user
sudo mysql -u root -p
```

```sql
CREATE DATABASE remart_scraper CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'remart_scraper'@'localhost' IDENTIFIED BY 'your_strong_password';
GRANT ALL PRIVILEGES ON remart_scraper.* TO 'remart_scraper'@'localhost';
FLUSH PRIVILEGES;
```

#### 3. Application Deployment

```bash
# Create application user
sudo useradd -m -s /bin/bash scraper
sudo usermod -aG sudo scraper

# Switch to application user
su - scraper

# Clone repository
git clone https://github.com/Ismat-Samadov/real_estate.git
cd real_estate

# Setup virtual environment
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure environment
cp .env.example .env
nano .env
```

#### 4. Environment Configuration

```env
# Database Configuration
DB_NAME=remart_scraper
DB_HOST=localhost
DB_USER=remart_scraper
DB_PASSWORD=your_strong_password
DB_PORT=3306

# Scraper Configuration
REQUEST_DELAY=1
MAX_RETRIES=5
LOGGING_LEVEL=INFO
SCRAPER_PAGES=2

# Proxy Configuration (if using Bright Data)
BRIGHT_DATA_USERNAME=your_username
BRIGHT_DATA_PASSWORD=your_password

# Server Configuration
SERVER_USER=scraper
SERVER_IP=your_server_ip
```

#### 5. Database Initialization
```bash
mysql -u remart_scraper -p remart_scraper < schema.sql
```

#### 6. Setup Systemd Service
```bash
sudo nano /etc/systemd/system/real-estate-scraper.service
```

```ini
[Unit]
Description=Real Estate Scraper Service
After=network.target

[Service]
User=scraper
Group=scraper
WorkingDirectory=/home/scraper/real_estate
Environment=PATH=/home/scraper/real_estate/.venv/bin
ExecStart=/home/scraper/real_estate/.venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start service
sudo systemctl enable real-estate-scraper
sudo systemctl start real-estate-scraper
```

#### 7. Logging Configuration
```bash
# Create log directory
sudo mkdir -p /var/log/real-estate-scraper
sudo chown scraper:scraper /var/log/real-estate-scraper

# Configure logrotate
sudo nano /etc/logrotate.d/real-estate-scraper
```

```conf
/var/log/real-estate-scraper/*.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 scraper scraper
    sharedscripts
    postrotate
        systemctl restart real-estate-scraper
    endscript
}
```

#### 8. Monitoring Setup
```bash
# Install monitoring tools
sudo apt install -y prometheus node-exporter

# Configure Prometheus for metrics
sudo nano /etc/prometheus/prometheus.yml
```

Add scraper job:
```yaml
scrape_configs:
  - job_name: 'real-estate-scraper'
    static_configs:
      - targets: ['localhost:8000']
```

#### 9. GitHub Actions Setup

1. Add repository secrets in GitHub:
   - `SERVER_IP`
   - `SERVER_USER`
   - `SSH_PRIVATE_KEY`
   - `DB_NAME`
   - `DB_HOST`
   - `DB_USER`
   - `DB_PASSWORD`
   - `BRIGHT_DATA_USERNAME`
   - `BRIGHT_DATA_PASSWORD`

2. Configure deployment workflow:
```yaml
name: Deploy Real Estate Scraper

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Deploy to Server
        uses: appleboy/ssh-action@master
        with:
          host: ${{ secrets.SERVER_IP }}
          username: ${{ secrets.SERVER_USER }}
          key: ${{ secrets.SSH_PRIVATE_KEY }}
          script: |
            cd /home/scraper/real_estate
            git pull
            source .venv/bin/activate
            pip install -r requirements.txt
            sudo systemctl restart real-estate-scraper
```

### Maintenance and Monitoring

#### Service Management
```bash
# Check service status
sudo systemctl status real-estate-scraper

# View logs
journalctl -u real-estate-scraper -f

# Restart service
sudo systemctl restart real-estate-scraper
```

#### Database Maintenance
```bash
# Backup database
mysqldump -u remart_scraper -p remart_scraper > backup-$(date +%F).sql

# Monitor database size
mysql -e "SELECT table_schema, 
    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'Size (MB)' 
    FROM information_schema.tables 
    WHERE table_schema = 'remart_scraper' 
    GROUP BY table_schema;"
```

#### Log Management
```bash
# Check log size
du -sh /var/log/real-estate-scraper/

# Analyze logs
tail -f /var/log/real-estate-scraper/scraper.log

# Force log rotation
sudo logrotate -f /etc/logrotate.d/real-estate-scraper
```

## 🛡️ Security Best Practices

1. **Firewall Configuration**
```bash
# Configure UFW
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

2. **SSL/TLS Setup**
```bash
# Install SSL certificate
sudo certbot --nginx -d your-domain.com
```

3. **Regular Updates**
```bash
# System updates
sudo apt update && sudo apt upgrade -y

# Python dependencies
pip install --upgrade -r requirements.txt
```

4. **Backup Strategy**
```bash
# Create backup script
nano /home/scraper/backup.sh
```

```bash
#!/bin/bash
BACKUP_DIR="/home/scraper/backups"
DATE=$(date +%Y%m%d)
mysqldump -u remart_scraper -p remart_scraper > "$BACKUP_DIR/db-$DATE.sql"
tar -czf "$BACKUP_DIR/code-$DATE.tar.gz" /home/scraper/real_estate
find "$BACKUP_DIR" -type f -mtime +7 -delete
```

```bash
# Add to crontab
0 0 * * * /home/scraper/backup.sh
```

## 📊 Monitoring Metrics

- **System Metrics:**
  - CPU usage
  - Memory consumption
  - Disk space
  - Network bandwidth

- **Application Metrics:**
  - Success rate per website
  - Average response time
  - Number of listings scraped
  - Database size growth
  - Error rates

- **Business Metrics:**
  - Total active listings
  - New listings per day
  - Price trends
  - Geographic distribution

## 🔍 Troubleshooting

### Common Issues and Solutions

1. **Service Won't Start**
```bash
# Check logs
journalctl -u real-estate-scraper -n 100

# Verify permissions
sudo chown -R scraper:scraper /home/scraper/real_estate
```

2. **Database Connectivity**
```bash
# Test connection
mysql -u remart_scraper -p -h localhost remart_scraper

# Check grants
SHOW GRANTS FOR 'remart_scraper'@'localhost';
```

3. **Memory Issues**
```bash
# Check memory usage
free -h
top -u scraper
```

4. **Deployment Failures**
```bash
# Check GitHub Actions logs
# Verify secrets
# Test SSH connection manually
ssh -i path/to/key scraper@your-server-ip
```

## 📫 Support and Maintenance

For ongoing support:
1. Check the issue tracker on GitHub
2. Review logs regularly
3. Monitor system resources
4. Maintain regular backups
5. Keep dependencies updated

For more information or support, contact:
- GitHub: [@Ismat-Samadov](https://github.com/Ismat-Samadov)
- Email: [ismetsemedov@gmail.com](mailto:ismetsemedov@gmail.com)