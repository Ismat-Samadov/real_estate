# test_scraper.py - Simple script to test individual scrapers
import os
import logging
import asyncio
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# Import all scrapers
from scrapers.arenda import OptimizedArendaScraper
from scrapers.ev10 import EV10Scraper
from scrapers.yeniemlak import YeniEmlakScraper
from scrapers.emlak import EmlakAzScraper
from scrapers.bina import OptimizedBinaScraper
from scrapers.ipoteka import IpotekaScraper
from scrapers.unvan import UnvanScraper
from scrapers.vipemlak import VipEmlakScraper
from scrapers.lalafo import LalafoScraper
from scrapers.tap import TapAzScraper

# Import proxy and database functions
from proxy.proxy_handler import DataImpulseProxyHandler
import mysql.connector
from mysql.connector import Error

# Set up dictionary mapping scraper names to classes
SCRAPERS = {
    'bina': OptimizedBinaScraper,
    'arenda': OptimizedArendaScraper,
    'tap': TapAzScraper,
    'emlak': EmlakAzScraper,
    'lalafo': LalafoScraper,
    'ev10': EV10Scraper,
    'unvan': UnvanScraper,
    'yeniemlak': YeniEmlakScraper,
    'ipoteka': IpotekaScraper,
    'vipemlak': VipEmlakScraper
}

def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection():
    """Create database connection"""
    try:
        load_dotenv()
        
        logger = logging.getLogger(__name__)
        logger.info("Connecting to database...")
        
        db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci',
            'use_unicode': True,
            'raise_on_warnings': True
        }
        
        connection = mysql.connector.connect(**db_config)
        
        # Set character set
        cursor = connection.cursor()
        cursor.execute('SET NAMES utf8mb4')
        cursor.execute('SET CHARACTER SET utf8mb4')
        cursor.execute('SET character_set_connection=utf8mb4')
        cursor.close()
        
        logger.info("Database connection established")
        return connection
        
    except Error as e:
        logger.error(f"Database connection error: {e}")
        raise

def validate_listing_data(listing):
    """Basic validation function for listings"""
    # This is a simplified version of the validation in main.py
    if not listing.get('listing_id') or not listing.get('source_url'):
        return {}
    
    # Ensure source_website is set
    if 'source_website' not in listing:
        listing['source_website'] = 'unknown'
    
    # Set created_at timestamp
    if 'created_at' not in listing:
        listing['created_at'] = datetime.now()
    
    # Set updated_at timestamp
    listing['updated_at'] = datetime.now()
    
    return listing

def save_listings_to_db(connection, listings):
    """Save listings to the database"""
    logger = logging.getLogger(__name__)
    stats = {'inserted': 0, 'failed': 0}
    
    if not listings:
        logger.warning("No listings to save")
        return stats
    
    try:
        cursor = connection.cursor(prepared=True)
        
        # First, check if the source_url exists
        check_query = "SELECT id FROM properties WHERE source_url = %s LIMIT 1"
        
        # Insert query with all fields
        insert_query = """
            INSERT INTO properties (
                listing_id, title, description, metro_station, district,
                address, location, latitude, longitude, rooms, area, 
                floor, total_floors, property_type, listing_type, price, 
                currency, contact_type, contact_phone, whatsapp_available,
                views_count, has_repair, amenities, photos,
                source_url, source_website, created_at, updated_at, 
                listing_date
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        
        # Update query for existing records
        update_query = """
            UPDATE properties SET
                title = %s, description = %s, metro_station = %s, district = %s,
                address = %s, location = %s, latitude = %s, longitude = %s, 
                rooms = %s, area = %s, floor = %s, total_floors = %s, 
                property_type = %s, listing_type = %s, price = %s, currency = %s, 
                contact_type = %s, contact_phone = %s, whatsapp_available = %s,
                views_count = %s, has_repair = %s, amenities = %s, photos = %s,
                source_website = %s, updated_at = %s, listing_date = %s
            WHERE source_url = %s
        """
        
        for listing in listings:
            try:
                # Validate listing data
                validated = validate_listing_data(listing)
                if not validated:
                    logger.warning(f"Invalid listing data: {listing.get('listing_id', 'unknown')}")
                    stats['failed'] += 1
                    continue
                
                # Check if record already exists
                cursor.execute(check_query, (validated.get('source_url'),))
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing record
                    update_values = (
                        validated.get('title'), validated.get('description'),
                        validated.get('metro_station'), validated.get('district'),
                        validated.get('address'), validated.get('location'),
                        validated.get('latitude'), validated.get('longitude'),
                        validated.get('rooms'), validated.get('area'),
                        validated.get('floor'), validated.get('total_floors'),
                        validated.get('property_type'), validated.get('listing_type'),
                        validated.get('price'), validated.get('currency'),
                        validated.get('contact_type'), validated.get('contact_phone'),
                        validated.get('whatsapp_available', False),
                        validated.get('views_count'), validated.get('has_repair', False),
                        validated.get('amenities'), validated.get('photos'),
                        validated.get('source_website'), validated.get('updated_at'),
                        validated.get('listing_date'), validated.get('source_url')
                    )
                    cursor.execute(update_query, update_values)
                    logger.info(f"Updated existing listing: {validated.get('listing_id')}")
                else:
                    # Insert new record
                    insert_values = (
                        validated.get('listing_id'), validated.get('title'),
                        validated.get('description'), validated.get('metro_station'),
                        validated.get('district'), validated.get('address'),
                        validated.get('location'), validated.get('latitude'),
                        validated.get('longitude'), validated.get('rooms'),
                        validated.get('area'), validated.get('floor'),
                        validated.get('total_floors'), validated.get('property_type'),
                        validated.get('listing_type'), validated.get('price'),
                        validated.get('currency'), validated.get('contact_type'),
                        validated.get('contact_phone'), validated.get('whatsapp_available', False),
                        validated.get('views_count'), validated.get('has_repair', False),
                        validated.get('amenities'), validated.get('photos'),
                        validated.get('source_url'), validated.get('source_website'),
                        validated.get('created_at'), validated.get('updated_at'),
                        validated.get('listing_date')
                    )
                    cursor.execute(insert_query, insert_values)
                    logger.info(f"Inserted new listing: {validated.get('listing_id')}")
                
                stats['inserted'] += 1
                
            except Exception as e:
                logger.error(f"Error saving listing {listing.get('listing_id', 'unknown')}: {str(e)}")
                stats['failed'] += 1
        
        connection.commit()
        cursor.close()
        
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        connection.rollback()
    
    return stats

async def run_scraper(scraper_name):
    """Run the specified scraper"""
    logger = logging.getLogger(__name__)
    connection = None
    
    try:
        # Validate scraper name
        scraper_name = scraper_name.lower().replace('.az', '')
        if scraper_name not in SCRAPERS:
            logger.error(f"Unknown scraper: {scraper_name}")
            print(f"Available scrapers: {', '.join(SCRAPERS.keys())}")
            return
        
        # Initialize database connection
        connection = get_db_connection()
        
        # Initialize proxy handler
        proxy_handler = DataImpulseProxyHandler()
        
        # Initialize and run the scraper
        logger.info(f"Starting {scraper_name} scraper")
        start_time = time.time()
        
        scraper_class = SCRAPERS[scraper_name]
        scraper = scraper_class()
        
        # Apply proxy to the scraper
        proxy_handler.apply_to_scraper(scraper)
        
        # Run the scraper with one page
        results = await scraper.run(pages=1)
        
        if not results:
            logger.warning("No results returned from scraper")
            return
        
        # Make sure source_website is set correctly
        for result in results:
            result['source_website'] = f"{scraper_name}.az"
        
        # Save results to database
        logger.info(f"Saving {len(results)} listings to database")
        stats = save_listings_to_db(connection, results)
        
        # Calculate and log metrics
        duration = time.time() - start_time
        avg_time = duration / len(results) if results else 0
        
        logger.info(f"Scraper completed in {duration:.2f}s")
        logger.info(f"Average time per listing: {avg_time:.2f}s")
        logger.info(f"Listings found: {len(results)}")
        logger.info(f"Successfully inserted/updated: {stats['inserted']}")
        logger.info(f"Failed: {stats['failed']}")
        
    except Exception as e:
        logger.error(f"Error running scraper: {str(e)}", exc_info=True)
    finally:
        if connection:
            connection.close()

async def main():
    """Main function"""
    logger = setup_logging()
    logger.info("Starting test scraper script")
    
    # Get scraper name from command line arguments or user input
    if len(sys.argv) > 1:
        scraper_name = sys.argv[1]
    else:
        print("Available scrapers:")
        for name in sorted(SCRAPERS.keys()):
            print(f"  - {name}")
        scraper_name = input("Enter scraper name: ")
    
    await run_scraper(scraper_name)

if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())