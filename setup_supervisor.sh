#!/bin/bash

echo "Stopping and cleaning up existing supervisord..."
sudo supervisorctl stop all
sudo systemctl stop supervisord
sudo pkill supervisord
sudo rm -f /etc/supervisord.d/*.ini
sudo rm -f /var/www/scraper/logs/supervisor*.log

echo "Creating logs directory..."
sudo mkdir -p /var/www/scraper/logs
sudo chown -R scraper:scraper /var/www/scraper/logs
sudo chmod 755 /var/www/scraper/logs

echo "Creating supervisor config..."
sudo tee /etc/supervisord.d/realestate_scraper.ini << 'EOF'
[program:realestate_scraper]
command=/var/www/scraper/venv/bin/python /var/www/scraper/main.py
directory=/var/www/scraper
user=scraper
autostart=true
autorestart=true
startretries=3
startsecs=10
redirect_stderr=true
stdout_logfile=/var/www/scraper/logs/supervisor.log
stderr_logfile=/var/www/scraper/logs/supervisor.err.log
environment=PYTHONPATH="/var/www/scraper",PYTHONUNBUFFERED="1"
EOF

echo "Updating main supervisord config..."
sudo grep -q '^\[include\]' /etc/supervisord.conf || echo -e '\n[include]\nfiles = /etc/supervisord.d/*.ini' | sudo tee -a /etc/supervisord.conf

echo "Starting supervisord..."
sudo systemctl enable supervisord
sudo systemctl start supervisord
sleep 5

echo "Starting scraper..."
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start realestate_scraper

echo "Checking status..."
sudo systemctl status supervisord
sudo supervisorctl status realestate_scraper
echo "Logs will appear at /var/www/scraper/logs/supervisor.log"