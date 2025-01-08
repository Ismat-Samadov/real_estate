import os
import logging
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from scrapers.arenda import OptimizedArendaScraper
import mysql.connector
from mysql.connector import Error
import datetime

def setup_logging():
    """Setup enhanced logging configuration"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Create a formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set up file handler
    file_handler = logging.FileHandler(log_dir / 'scraper.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[file_handler, console_handler]
    )
    
    return logging.getLogger(__name__)

def get_db_connection():
    """Create database connection"""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get database configuration from environment variables
        db_config = {
            'host': os.getenv('DB_HOST', 'sql7.freemysqlhosting.net'),
            'user': os.getenv('DB_USER', 'sql7756502'),
            'password': os.getenv('DB_PASSWORD', 'hvh9kAbLZA'),
            'database': os.getenv('DB_NAME', 'sql7756502'),
            'raise_on_warnings': True
        }
        
        # Create the connection
        connection = mysql.connector.connect(**db_config)
        return connection
    except Error as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def save_listings_to_db(connection, listings):
    """Save listings to database with handling for missing fields"""
    cursor = connection.cursor()
    successful = 0
    failed = 0
    logger = logging.getLogger(__name__)
    
    for listing in listings:
        try:
            # Ensure all required fields exist with defaults
            sanitized_listing = {
                'listing_id': listing.get('listing_id'),
                'title': listing.get('title'),
                'description': listing.get('description'),
                'metro_station': listing.get('metro_station'),
                'district': listing.get('district'),
                'address': listing.get('address'),
                'location': listing.get('location'),
                'rooms': listing.get('rooms', None),  # Default to NULL if missing
                'area': listing.get('area', None),
                'floor': listing.get('floor', None),
                'total_floors': listing.get('total_floors', None),
                'property_type': listing.get('property_type', 'unknown'),
                'listing_type': listing.get('listing_type', 'unknown'),
                'price': listing.get('price', 0),
                'currency': listing.get('currency', 'AZN'),
                'contact_phone': listing.get('contact_phone'),
                'whatsapp_available': listing.get('whatsapp_available', False),
                'source_url': listing.get('source_url'),
                'source_website': listing.get('source_website', 'arenda.az'),
                'created_at': listing.get('created_at', datetime.datetime.now()),
                'updated_at': listing.get('updated_at', datetime.datetime.now())
            }
            
            # Prepare the SQL query
            insert_query = """
                INSERT INTO properties (
                    listing_id, title, description, metro_station, district,
                    address, location, rooms, area, floor, total_floors,
                    property_type, listing_type, price, currency,
                    contact_phone, whatsapp_available, source_url,
                    source_website, created_at, updated_at
                ) VALUES (
                    %(listing_id)s, %(title)s, %(description)s, %(metro_station)s,
                    %(district)s, %(address)s, %(location)s, %(rooms)s,
                    %(area)s, %(floor)s, %(total_floors)s, %(property_type)s,
                    %(listing_type)s, %(price)s, %(currency)s, %(contact_phone)s,
                    %(whatsapp_available)s, %(source_url)s, %(source_website)s,
                    %(created_at)s, %(updated_at)s
                ) ON DUPLICATE KEY UPDATE
                    updated_at = VALUES(updated_at),
                    price = VALUES(price),
                    title = VALUES(title),
                    description = VALUES(description)
            """
            
            cursor.execute(insert_query, sanitized_listing)
            successful += 1
            
        except Exception as e:
            failed += 1
            logger.error(f"Error saving listing {listing.get('listing_id')}: {str(e)}")
            continue
    
    connection.commit()
    cursor.close()
    
    logger.info(f"Successfully saved {successful} listings")
    if failed > 0:
        logger.warning(f"Failed to save {failed} listings")

async def main():
    """Main async function to run the scraper"""
    logger = setup_logging()
    logger.info("Starting scraper application")
    connection = None
    
    try:
        # Initialize and run scraper
        scraper = OptimizedArendaScraper()
        logger.info("Scraper initialized successfully")
        
        # Get database connection
        try:
            connection = get_db_connection()
            logger.info("Database connection established successfully")
        except mysql.connector.Error as err:
            logger.error(f"Failed to connect to database: {err}")
            # Continue without database to at least test scraping
            logger.info("Continuing without database to test scraping functionality")
        
        # Run scraper and get results
        results = await scraper.run(pages=1)
        logger.info(f"Successfully scraped {len(results)} listings")
        
        # Save results to database if we have a connection
        if connection:
            save_listings_to_db(connection, results)
            logger.info("Data saved to database successfully")
        else:
            # Log the scraped data for debugging
            logger.info(f"Scraped {len(results)} listings successfully")
            logger.debug("First listing sample: %s", results[0] if results else "No results")
        
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}", exc_info=True)
        raise
    finally:
        if 'connection' in locals():
            connection.close()
        logger.info("Scraping application shutting down")

if __name__ == "__main__":
    asyncio.run(main())