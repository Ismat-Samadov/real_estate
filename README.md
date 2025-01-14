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

## 🛠️ Technical Requirements

- Python 3.10 or higher
- MySQL Server 8.0+
- 2GB RAM minimum
- Stable internet connection
- Linux/macOS/Windows

## 📦 Installation

1. **Clone and Setup**
   ```bash
   git clone https://github.com/Ismat-Samadov/real_estate.git
   cd real_estate
   python -m venv .venv
   
   # Linux/macOS
   source .venv/bin/activate
   
   # Windows
   .venv\Scripts\activate
   
   pip install -r requirements.txt
   ```

2. **Environment Configuration**
   Create a `.env` file with the following variables:

   ```env
   # Database Configuration
   DB_HOST=your_database_host
   DB_USER=your_database_user
   DB_PASSWORD=your_database_password
   DB_NAME=your_database_name
   PORT=27566
   SSL_CERT=your_ssl_certificate

   # Scraper Configuration
   REQUEST_DELAY=1
   MAX_RETRIES=5
   LOGGING_LEVEL=INFO
   SCRAPER_PAGES=2

   # Optional Features
   ENABLE_PROXY=false
   PROXY_ROTATION_INTERVAL=600
   ```

3. **Database Setup**
   ```bash
   mysql -u your_user -p your_database < schema.sql
   ```

## 🚀 Usage

### Basic Operation
```bash
python main.py
```

### Command Line Options (Coming Soon)
```bash
# Scrape specific sites
python main.py --sites bina,emlak

# Set custom page limits
python main.py --pages 5

# Export data
python main.py --export csv
```

## 📁 Project Structure

```
real_estate/
├── LICENSE             # MIT License
├── README.md          # Documentation
├── ca.pem             # SSL Certificate
├── logs/              # Application logging
│   └── scraper.log    # Detailed logs
├── main.py            # Application entry point
├── requirements.txt   # Dependencies
├── schema.sql        # Database schema
└── scrapers/         # Individual site scrapers
    ├── arenda.py     # Arenda.az implementation
    ├── bina.py       # Bina.az implementation
    ├── emlak.py      # Emlak.az implementation
    ├── ev10.py       # EV10.az implementation
    ├── ipoteka.py    # Ipoteka.az implementation
    ├── lalafo.py     # Lalafo.az implementation
    ├── tap.py        # Tap.az implementation
    ├── unvan.py      # Unvan.az implementation
    ├── vipemlak.py   # VipEmlak.az implementation
    └── yeniemlak.py  # YeniEmlak.az implementation
```

## 💾 Database Schema

### Properties Table
```sql
CREATE TABLE properties (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    listing_id VARCHAR(50) UNIQUE,
    title VARCHAR(200),
    price DECIMAL(12, 2),
    rooms SMALLINT,
    area DECIMAL(10, 2),
    ... # See schema.sql for complete definition
);
```

## 🔍 Monitoring

### Logging
- Application logs: `logs/scraper.log`
- Database operations log
- Performance metrics
- Error tracking

### Metrics (Coming Soon)
- Success/failure rates
- Response times
- Data quality scores
- Coverage statistics

## 🛡️ Best Practices & Safety

- Rate limiting enforcement
- Respectful crawling
- Data privacy compliance
- Error recovery
- Connection pooling
- User-agent rotation

## 🔄 Development Workflow

1. Create feature branch
2. Implement changes
3. Add tests
4. Update documentation
5. Submit pull request
6. Code review
7. Merge to main

## 🎯 Roadmap

### Current Status ✅
- [x] Completed all major property website scrapers
- [x] Implemented database schema and storage
- [x] Set up logging and error handling
- [x] Added GitHub Actions automation

### Next Steps
- [ ] Implement proxy rotation for enhanced reliability
- [ ] Add data analytics and visualization dashboard
- [ ] Create REST API for data access
- [ ] Add automated testing suite
- [ ] Implement caching layer
- [ ] Add data export functionality
- [ ] Create admin monitoring interface
- [ ] Implement real-time price tracking
- [ ] Add email notifications for price changes

## 🤝 Contributing

Contributions are welcome! See our [Contributing Guide](CONTRIBUTING.md) for details.

## 📄 License

This project is MIT licensed - see [LICENSE](LICENSE) for details.

## 👥 Team

- **Ismat Samadov** - Project Lead
  - GitHub: [@Ismat-Samadov](https://github.com/Ismat-Samadov)
  - Email: [ismetsemedov@gmail.com](mailto:ismetsemedov@gmail.com)

## 🛠️ Built With

- [Python](https://www.python.org/) - Core language
- [aiohttp](https://docs.aiohttp.org/) - Async HTTP
- [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/) - HTML parsing
- [MySQL Connector](https://dev.mysql.com/doc/connector-python/en/) - Database
- [GitHub Actions](https://github.com/features/actions) - CI/CD

## 📊 Stats

- Lines of code: 5,000+
- Active sites: 5
- Daily listings: ~10,000
- Database size: Growing

## 📫 Support

Need help? Open an issue or contact the team.