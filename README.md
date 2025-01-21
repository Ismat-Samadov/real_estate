# Real Estate Data Scraper 🏠

A high-performance Python web scraper for collecting and analyzing real estate listings from major Azerbaijani property websites. Built with asyncio for maximum efficiency and scalability.

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Scraping](https://github.com/Ismat-Samadov/real_estate/actions/workflows/scraper.yaml/badge.svg)](https://github.com/Ismat-Samadov/real_estate/actions/workflows/scraper.yaml)

## 🤔 What It Does

This project automatically collects real estate listings data from major Azerbaijani property websites. For each property listing, it gathers:

- Basic Details: Price, number of rooms, floor level, total area
- Location Info: Address, district, nearby metro stations, coordinates
- Property Features: Repairs status, amenities, building type
- Media Content: Photos, descriptions
- Contact Info: Phone numbers, agent/owner status
- Metadata: Listing date, view count, updates

All this data is stored in a structured MySQL database for analysis and tracking.

## 🔄 How It Works

1. **Initialization**
   - Loads configuration from `.env` file
   - Establishes database connection
   - Sets up logging system

2. **Scraping Process**
   - Creates async sessions for each website
   - Implements site-specific parsing logic
   - Handles pagination and listing details
   - Manages rate limiting and retries

3. **Data Processing**
   - Validates and cleans extracted data
   - Standardizes formats across sources
   - Handles different currencies and units
   - Processes and stores media content

4. **Storage**
   - Checks for duplicate listings
   - Updates existing records
   - Maintains data integrity
   - Logs operations and errors

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
- Asynchronous multi-site scraping for efficiency
- Smart rate limiting to avoid blocking
- Anti-bot detection avoidance
- Automatic retry on failures
- Connection pooling for stability

### Data Processing
- Full property details extraction
- Location data with geocoding
- Real-time price tracking
- Contact information validation
- Photo URL processing

### Infrastructure
- Robust MySQL database storage
- Comprehensive logging
- Automated GitHub Actions
- Environment-based config
- Security measures

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
├── LICENSE                 # MIT License
├── README.md              # Documentation
├── bright_data_proxy.py   # Proxy configuration
├── logs/                  # Application logging
│   └── scraper.log        # Detailed logs
├── main.py                # Application entry point
├── monitoring.sql         # Monitoring queries
├── requirements.txt       # Python dependencies
├── run_scraper.sh        # Shell script for running scraper
├── schema.sql            # Database schema
└── scrapers/             # Individual site scrapers
    ├── arenda.py         # Arenda.az implementation
    ├── bina.py           # Bina.az implementation
    ├── emlak.py          # Emlak.az implementation
    ├── ev10.py           # EV10.az implementation
    ├── ipoteka.py        # Ipoteka.az implementation
    ├── lalafo.py         # Lalafo.az implementation
    ├── tap.py            # Tap.az implementation
    ├── unvan.py          # Unvan.az implementation
    ├── vipemlak.py       # VipEmlak.az implementation
    └── yeniemlak.py      # YeniEmlak.az implementation
```

## 📊 Data Collection Process

1. **Site Selection**
   - Each scraper module targets specific website
   - Handles unique site structure
   - Manages site-specific features

2. **Data Extraction**
   - Parses HTML with BeautifulSoup4
   - Handles different page layouts
   - Extracts structured data

3. **Validation & Storage**
   - Cleans and validates data
   - Standardizes formats
   - Stores in MySQL database

4. **Error Handling**
   - Retries failed requests
   - Logs errors and warnings
   - Maintains data integrity

## 🛡️ Best Practices

- Rate limiting to respect servers
- Proper user-agent identification
- Data privacy compliance
- Error recovery mechanisms
- Connection pooling
- Regular maintenance

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

- [Python](https://www.python.org/) - Core language
- [aiohttp](https://docs.aiohttp.org/) - Async HTTP requests
- [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/) - HTML parsing
- [MySQL Connector](https://dev.mysql.com/doc/connector-python/en/) - Database operations
- [GitHub Actions](https://github.com/features/actions) - CI/CD automation