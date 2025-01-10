# Real Estate Data Scraper

A robust Python-based web scraper that collects and analyzes real estate listings from major Azerbaijani property websites.

- [bina.az](https://bina.az)
- [yeniemlak.az](https://yeniemlak.az)
- [emlak.az](https://emlak.az)
- [lalafo.az](https://lalafo.az)
- [tap.az](https://tap.az)
- [ev10.az](https://ev10.az)

## Key Features

- Asynchronous web scraping with comprehensive error handling and retry mechanisms
- Intelligent rate limiting and anti-bot detection avoidance
- Extensive data extraction including:
  - Property details (type, price, rooms, area)
  - Location information (address, district, metro station)
  - Contact information and availability
  - Media content (photos, descriptions)
- Automated MySQL database integration
- Comprehensive logging system
- GitHub Actions workflow for automated scraping
- Environment-based configuration

## Technical Requirements

- Python 3.10+
- MySQL Server
- Virtual environment
- Required Python packages (see `requirements.txt`)

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/Ismat-Samadov/real_estate.git
cd real_estate
```

2. Set up Python environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
pip install -r requirements.txt
```

3. Configure environment variables in `.env`:
```env
DB_HOST=your_database_host
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_NAME=your_database_name
REQUEST_DELAY=1
MAX_RETRIES=5
LOGGING_LEVEL=INFO
SCRAPER_PAGES=2
```

4. Initialize database:
```bash
mysql -u your_user -p your_database < schema.sql
```

5. Run the scraper:
```bash
python main.py
```

## Project Structure

```
real_estate/
├── .github/
│   └── workflows/
│       └── scraper.yaml    # GitHub Actions workflow
├── scrapers/
│   ├── arenda.py          # Arenda.az scraper
│   └── ev10.py            # Future ev10.az scraper
├── logs/
│   └── scraper.log        # Application logs
├── main.py                # Application entry point
├── requirements.txt       # Python dependencies
├── schema.sql            # Database schema
└── README.md             # This file
```

## Database Schema

The `properties` table includes:

- Primary identifiers (`id`, `listing_id`)
- Property details (`title`, `rooms`, `area`, `floor`, `total_floors`)
- Location information (`address`, `district`, `metro_station`)
- Pricing data (`price`, `currency`, `listing_type`)
- Contact information (`contact_phone`, `whatsapp_available`)
- Rich content (`description`, `amenities`, `photos`)
- Metadata (`created_at`, `updated_at`, `source_url`, `source_website`)

## Advanced Features

### Error Handling
- Exponential backoff for failed requests
- Automatic retry mechanism for database operations
- Comprehensive error logging
- Data validation before storage
- Rate limiting and request throttling

### Logging System
- Detailed logging in `logs/scraper.log`
- Request/response tracking
- Error tracing with stack traces
- Performance metrics
- Database operation logging

### GitHub Actions Integration
- Automated hourly scraping
- Configurable schedule
- Artifact upload for logs
- Secret management for database credentials

## Best Practices & Safety

- Respect `robots.txt` directives
- Implement rate limiting
- Follow website terms of service
- Avoid scraping personal data
- GDPR-compliant data storage
- User-agent rotation
- Connection pooling
- Error recovery mechanisms

## Development Roadmap

- [ ] Add support for additional websites
- [ ] Implement data analysis features
- [ ] Create API endpoints
- [ ] Add proxy rotation
- [ ] Enhance data validation
- [ ] Add automated testing
- [ ] Implement data visualization
- [ ] Add caching layer
- [ ] Create admin dashboard

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/NewFeature`)
3. Commit changes (`git commit -m 'Add NewFeature'`)
4. Push to branch (`git push origin feature/NewFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**Ismat Samadov**
- GitHub: [@Ismat-Samadov](https://github.com/Ismat-Samadov)

## Technologies Used

- Python (asyncio, aiohttp)
- MySQL
- BeautifulSoup4
- GitHub Actions
- Environment management
- Logging framework