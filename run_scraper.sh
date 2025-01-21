set -e
cd /var/www/scraper
mkdir -p logs
echo "[$(date)] Starting scraper job" >> logs/cron.log
if [ ! -d "venv" ]; then
    echo "[$(date)] Creating virtual environment" >> logs/cron.log
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
else
    source venv/bin/activate
fi
echo "[$(date)] Running scraper" >> logs/cron.log
python main.py
deactivate
echo "[$(date)] Job completed" >> logs/cron.log
