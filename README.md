# Real Estate Data Scraper

A robust Python-based web scraper designed to collect and store real estate listings from Azerbaijani property websites, with initial support for Arenda.az.

## Features

- Automated scraping of property listings with extensive error handling
- Robust rate limiting and retry mechanisms
- Comprehensive data extraction including:
  - Basic property information (title, type, price)
  - Location details (address, district, metro station)
  - Property specifications (rooms, area, floor)
  - Contact information
  - Photos and descriptions
- MySQL database integration for data storage
- Detailed logging system
- Environment-based configuration

## Prerequisites

- Python 3.8+
- MySQL Server
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Ismat-Samadov/real_estate.git
cd real_estate
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Set up your environment variables by creating a `.env` file:
```env
DB_HOST=your_database_host
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_NAME=your_database_name
REQUEST_DELAY=1
MAX_RETRIES=5
LOGGING_LEVEL=INFO
```

5. Create the database schema using the provided `schema.sql` file:
```bash
mysql -u your_user -p your_database < schema.sql
```

## Project Structure

```
real_estate/
├── LICENSE
├── README.md
├── logs/
│   └── scraper.log
├── main.py
├── requirements.txt
└── schema.sql
```

## Usage

Run the scraper:
```bash
python main.py
```

The scraper will:
1. Start collecting listings from Arenda.az
2. Process each listing to extract detailed information
3. Store the data in the configured MySQL database
4. Log all activities in `logs/scraper.log`

## Database Schema

The scraper stores data in a `properties` table with the following key fields:
- `id`: Auto-incrementing primary key
- `listing_id`: Unique identifier from the source website
- `title`: Property title/headline
- `property_type`: Type of property
- `listing_type`: Category (daily/monthly/sale)
- `price`: Property price
- `rooms`: Number of rooms
- `area`: Total area in square meters
- `location`: Property location
- Various timestamps and metadata fields

## Error Handling

The scraper implements several layers of error handling:
- Request retries with exponential backoff
- Database connection retry mechanism
- Comprehensive error logging
- Data validation before storage
- Rate limiting to prevent server overload

## Logging

Logs are stored in `logs/scraper.log` and include:
- Scraping progress and statistics
- Error messages and stack traces
- Database operations
- Request/response information

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Safety and Legal Considerations

- Always respect the target website's robots.txt file
- Implement appropriate rate limiting
- Be mindful of the website's terms of service
- Do not scrape personal or sensitive information
- Store data in compliance with relevant data protection regulations

## Future Improvements

- Add support for more real estate websites
- Implement data analysis and visualization features
- Add API endpoints for data access
- Enhance data validation and cleaning
- Add support for proxy rotation
- Implement automated testing

## Acknowledgments

- Built with Python and MySQL
- Uses BeautifulSoup4 for HTML parsing
- Implements best practices for web scraping

## Author

Ismat Samadov

## Contact

- GitHub: [@Ismat-Samadov](https://github.com/Ismat-Samadov)