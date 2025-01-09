import os
import logging
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from scrapers.arenda import OptimizedArendaScraper
from scrapers.ev10 import EV10Scraper
import mysql.connector
from mysql.connector import Error
import datetime

def setup_logging():
    """Setup enhanced logging configuration"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    file_handler = logging.FileHandler(log_dir / 'scraper.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[file_handler, console_handler]
    )
    
    return logging.getLogger(__name__)

def get_db_connection():
    """Create database connection"""
    try:
        load_dotenv()
        
        db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'raise_on_warnings': True
        }
        
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
            sanitized_listing = {
                'listing_id': listing.get('listing_id'),
                'title': listing.get('title'),
                'description': listing.get('description'),
                'metro_station': listing.get('metro_station'),
                'district': listing.get('district'),
                'address': listing.get('address'),
                'location': listing.get('location'),
                'rooms': listing.get('rooms'),
                'area': listing.get('area'),
                'floor': listing.get('floor'),
                'total_floors': listing.get('total_floors'),
                'property_type': listing.get('property_type', 'unknown'),
                'listing_type': listing.get('listing_type', 'unknown'),
                'price': listing.get('price', 0),
                'currency': listing.get('currency', 'AZN'),
                'contact_phone': listing.get('contact_phone'),
                'whatsapp_available': listing.get('whatsapp_available', False),
                'source_url': listing.get('source_url'),
                'source_website': listing.get('source_website'),
                'created_at': listing.get('created_at', datetime.datetime.now()),
                'updated_at': listing.get('updated_at', datetime.datetime.now())
            }
            
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

async def run_scrapers():
    """Run all scrapers and aggregate results"""
    logger = logging.getLogger(__name__)
    all_results = []
    
    # Get configuration from environment
    load_dotenv()
    pages = int(os.getenv('SCRAPER_PAGES', 2))  # Default to 2 pages if not set
    
    scrapers = [
        # ("Arenda.az", OptimizedArendaScraper()),
        ("EV10.az", EV10Scraper())
    ]
    
    logger.info(f"Starting scrapers with {pages} pages each")
    
    for name, scraper in scrapers:
        try:
            logger.info(f"Starting {name} scraper for {pages} pages")
            results = await scraper.run(pages=pages)
            logger.info(f"{name} scraper completed: {len(results)} listings from {pages} pages")
            all_results.extend(results)
        except Exception as e:
            logger.error(f"Error running {name} scraper: {str(e)}", exc_info=True)
    
    return all_results

async def main():
    """Main async function to run scrapers"""
    logger = setup_logging()
    logger.info("Starting scraper application")
    connection = None
    
    try:
        try:
            connection = get_db_connection()
            logger.info("Database connection established")
        except mysql.connector.Error as err:
            logger.error(f"Database connection failed: {err}")
            logger.info("Continuing to test scraping")
        
        results = await run_scrapers()
        logger.info(f"All scrapers completed. Total listings: {len(results)}")
        
        if connection and results:
            save_listings_to_db(connection, results)
            logger.info("Data saved to database")
        elif results:
            logger.info(f"Scraped {len(results)} listings")
            logger.debug("Sample: %s", results[0])
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        raise
    finally:
        if connection:
            connection.close()
        logger.info("Application shutting down")

if __name__ == "__main__":
    asyncio.run(main())