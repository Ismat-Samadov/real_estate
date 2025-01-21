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

## ✨ Key Features

### Data Collection
- Asynchronous multi-site scraping
- Intelligent rate limiting
- Anti-bot detection avoidance
- Automatic retry mechanisms
- Connection pooling

### Data Processing
- Comprehensive property details
- Location data with geocoding
- Price analysis and tracking
- Contact information validation
- Media content processing

### Infrastructure
- MySQL database integration
- Structured logging system
- GitHub Actions automation
- Environment-based config
- SSL/TLS security

## 🛠️ Requirements

- Python 3.10+
- MySQL Server 8.0+
- Virtual environment
- Git

## 📦 Installation

1. **Clone Repository**
   ```bash
   git clone https://github.com/Ismat-Samadov/real_estate.git
   cd real_estate
   ```

2. **Setup Python Environment**
   ```bash
   python -m venv .venv

   # Linux/macOS
   source .venv/bin/activate

   # Windows
   .venv\Scripts\activate

   pip install -r requirements.txt
   ```

3. **Configure Environment**
   Create `.env` file:
   ```env
   # Database
   DB_NAME=remart_scraper
   DB_HOST=your_host
   DB_USER=your_user
   DB_PASSWORD=your_password
   DB_PORT=3306

   # Scraper
   REQUEST_DELAY=1
   MAX_RETRIES=5
   LOGGING_LEVEL=INFO
   SCRAPER_PAGES=2

   # Proxy (optional)
   BRIGHT_DATA_USERNAME=your_username
   BRIGHT_DATA_PASSWORD=your_password
   ```

4. **Initialize Database**
   ```bash
   mysql -u your_user -p your_database < schema.sql
   ```

5. **Run Scraper**
   ```bash
   python main.py
   ```

## 📁 Project Structure

```
real_estate/
├── LICENSE             # MIT License
├── README.md          # Documentation
├── logs/              # Application logging
│   └── scraper.log    # Detailed logs
├── main.py            # Entry point
├── requirements.txt   # Dependencies
├── schema.sql         # Database schema
└── scrapers/          # Site scrapers
    ├── arenda.py      # Arenda.az
    ├── bina.py        # Bina.az
    ├── emlak.py       # Emlak.az
    ├── ev10.py        # EV10.az
    ├── ipoteka.py     # Ipoteka.az
    ├── lalafo.py      # Lalafo.az
    ├── tap.py         # Tap.az
    ├── unvan.py       # Unvan.az
    ├── vipemlak.py    # VipEmlak.az
    └── yeniemlak.py   # YeniEmlak.az
```

## 🔍 Monitoring

- Application logs: `logs/scraper.log`
- Database operations log
- Error tracking
- Performance metrics

## 🛡️ Best Practices

- Rate limiting enforcement
- Respectful crawling
- Data privacy compliance
- Error recovery
- Connection pooling
- User-agent rotation

## 🎯 Roadmap

- [x] Core scraper implementation
- [x] Database integration
- [x] Logging system
- [x] GitHub Actions automation
- [ ] Proxy rotation
- [ ] Data analytics dashboard
- [ ] REST API
- [ ] Testing suite
- [ ] Caching layer
- [ ] Export functionality
- [ ] Admin interface
- [ ] Price tracking
- [ ] Email notifications

## 📄 License

This project is MIT licensed - see [LICENSE](LICENSE) for details.

## 👥 Author

**Ismat Samadov**
- GitHub: [@Ismat-Samadov](https://github.com/Ismat-Samadov)
- Email: [ismetsemedov@gmail.com](mailto:ismetsemedov@gmail.com)

## 🛠️ Built With

- [Python](https://www.python.org/)
- [aiohttp](https://docs.aiohttp.org/)
- [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/)
- [MySQL Connector](https://dev.mysql.com/doc/connector-python/en/)
- [GitHub Actions](https://github.com/features/actions)