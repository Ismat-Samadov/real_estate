#!/bin/bash

# Print commands and their arguments as they are executed
set -x

# Exit on any error
set -e

# Store sudo password
SUDO_PASSWORD="Scrapper@1213"

# Setup script for real estate scraper cron job
echo "Setting up cron job for real estate scraper..."

# Function to run sudo commands with password
sudo_cmd() {
    echo $SUDO_PASSWORD | sudo -S $@
}

# Create the run script
sudo_cmd bash -c 'cat > /var/www/scraper/run_scraper.sh << '\''EOL'\''
#!/bin/bash
set -e

# Set working directory
cd /var/www/scraper

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Run the scraper
python main.py

# Deactivate virtual environment
deactivate
EOL'

# Make run script executable
sudo_cmd chmod +x /var/www/scraper/run_scraper.sh

# Create logs directory and set permissions
sudo_cmd mkdir -p /var/www/scraper/logs
sudo_cmd chown -R scraper:scraper /var/www/scraper/logs

# Set timezone to Asia/Baku
sudo_cmd timedatectl set-timezone Asia/Baku

# Create new crontab entry
(crontab -l 2>/dev/null || true; echo "0 */2 * * * /var/www/scraper/run_scraper.sh >> /var/www/scraper/logs/cron.log 2>&1") | crontab -

echo "Cron job setup complete! The scraper will run every 2 hours."
echo "You can check the logs at /var/www/scraper/logs/cron.log"