import os
import logging
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from scrapers.arenda import OptimizedArendaScraper
from scrapers.ev10 import EV10Scraper
from scrapers.yeniemlak import YeniEmlakScraper
from scrapers.emlak import EmlakAzScraper
from scrapers.bina import BinaScraper
import mysql.connector
from mysql.connector import Error
import datetime
import tempfile
from typing import Dict, List, Optional, Tuple

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
    """Create database connection with SSL configuration"""
    try:
        load_dotenv()
        
        # Read certificate content and clean up any potential whitespace issues
        cert_content = os.getenv('SSL_CERT', '').strip()
        if not cert_content:
            raise ValueError("SSL certificate not found in environment variables")
            
        # Create a temporary file for the certificate
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as cert_file:
            cert_file.write(cert_content)
            cert_file.flush()  # Ensure all data is written to disk
            cert_path = cert_file.name

        try:
            db_config = {
                'host': os.getenv('DB_HOST'),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'database': os.getenv('DB_NAME'),
                'port': int(os.getenv('PORT', '27566')),
                'raise_on_warnings': True,
                'ssl_ca': cert_path,
                'ssl_verify_cert': True,
            }
            
            connection = mysql.connector.connect(**db_config)
            logging.info("Successfully connected to database with SSL")
            return connection
            
        finally:
            # Clean up the temporary file
            try:
                os.unlink(cert_path)
            except Exception as e:
                logging.warning(f"Failed to remove temporary certificate file: {e}")
        
    except Error as e:
        logging.error(f"Error connecting to database: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise


def ensure_connection(connection):
    """Ensure database connection is alive and reconnect if needed"""
    try:
        connection.ping(reconnect=True, attempts=3, delay=5)
    except mysql.connector.Error as err:
        logger = logging.getLogger(__name__)
        logger.error(f"Database connection error: {err}")
        try:
            connection = get_db_connection()
        except mysql.connector.Error as err:
            logger.error(f"Failed to reconnect to database: {err}")
            raise
    return connection

def debug_listing_data(listing: Dict, listing_id: str) -> None:
    """Debug log the raw listing data before validation"""
    logger = logging.getLogger(__name__)
    debug_fields = ['latitude', 'longitude', 'floor', 'rooms', 'area']
    
    logger.debug(f"Raw data for listing {listing_id}:")
    for field in debug_fields:
        if field in listing:
            logger.debug(f"{field}: {type(listing[field])} = {listing[field]}")


from decimal import Decimal, ROUND_HALF_UP
import json
import logging
from typing import Dict, Optional
import re

def validate_numeric_field(value: any, field_name: str, min_val: float = None, max_val: float = None) -> Optional[float]:
    """Validate and convert numeric fields with enhanced precision handling"""
    logger = logging.getLogger(__name__)
    
    if value is None:
        return None
        
    # Convert bytes to string if necessary
    if isinstance(value, bytes):
        try:
            value = value.decode('utf-8')
        except UnicodeDecodeError:
            logger.error(f"Failed to decode bytes value for {field_name}")
            return None
    
    # Clean up string input
    if isinstance(value, str):
        # Remove any non-numeric characters except decimal point and minus
        value = re.sub(r'[^\d.-]', '', value.strip())
        if not value:
            return None
    
    try:
        # Convert to high-precision decimal first
        if isinstance(value, str):
            num_value = Decimal(value)
        else:
            num_value = Decimal(str(float(value)))
        
        # Apply range checks
        if min_val is not None and num_value < Decimal(str(min_val)):
            logger.debug(f"{field_name} value {num_value} below minimum {min_val}")
            return None
        if max_val is not None and num_value > Decimal(str(max_val)):
            logger.debug(f"{field_name} value {num_value} above maximum {max_val}")
            return None
        
        # Handle specific field types
        if field_name in ['rooms', 'floor', 'total_floors', 'views_count']:
            # For integer fields, round to nearest integer
            int_value = int(num_value.quantize(Decimal('1'), rounding=ROUND_HALF_UP))
            logger.debug(f"Converting {field_name} from {num_value} to int: {int_value}")
            return int_value
            
        elif field_name in ['latitude', 'longitude']:
            # For coordinates, preserve 8 decimal places
            coord_value = float(num_value.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP))
            logger.debug(f"Processing {field_name}: {num_value} -> {coord_value}")
            return coord_value
            
        else:
            # For other numeric fields (price, area), keep 2 decimal places
            float_value = float(num_value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
            logger.debug(f"Converting {field_name} to 2 decimal places: {float_value}")
            return float_value
            
    except Exception as e:
        logger.error(f"Error converting {field_name} value: {value} ({type(value)}): {str(e)}")
        return None

def validate_listing_data(listing: Dict) -> Dict:
    """Validate and clean listing data before database insertion"""
    logger = logging.getLogger(__name__)
    validated = listing.copy()
    
    logger.debug(f"Validating listing {listing.get('listing_id')}")
    
    # Required fields validation
    if not validated.get('listing_id'):
        logger.error("Missing required listing_id")
        return {}
    
    # Coordinate validation
    if 'latitude' in validated:
        validated['latitude'] = validate_numeric_field(
            validated['latitude'], 'latitude', min_val=-90, max_val=90)
            
    if 'longitude' in validated:
        validated['longitude'] = validate_numeric_field(
            validated['longitude'], 'longitude', min_val=-180, max_val=180)
    
    # Area validation
    if 'area' in validated:
        validated['area'] = validate_numeric_field(
            validated['area'], 'area', min_val=5, max_val=10000)
    
    # Rooms validation
    if 'rooms' in validated:
        validated['rooms'] = validate_numeric_field(
            validated['rooms'], 'rooms', min_val=1, max_val=50)
    
    # Floor validation
    if 'floor' in validated:
        validated['floor'] = validate_numeric_field(
            validated['floor'], 'floor', min_val=0, max_val=200)
    
    # Total floors validation
    if 'total_floors' in validated:
        validated['total_floors'] = validate_numeric_field(
            validated['total_floors'], 'total_floors', min_val=1, max_val=200)
    
    # Price validation
    if 'price' in validated:
        validated['price'] = validate_numeric_field(
            validated['price'], 'price', min_val=0, max_val=100000000)
    
    # Views count validation
    if 'views_count' in validated:
        validated['views_count'] = validate_numeric_field(
            validated['views_count'], 'views_count', min_val=0)
    
    # Ensure floor doesn't exceed total_floors
    if (validated.get('floor') is not None and 
        validated.get('total_floors') is not None and 
        validated['floor'] > validated['total_floors']):
        logger.warning(f"Floor {validated['floor']} exceeds total_floors {validated['total_floors']}")
        validated['floor'] = None
        validated['total_floors'] = None
    
    # Convert boolean fields
    for bool_field in ['whatsapp_available', 'has_repair']:
        if bool_field in validated:
            validated[bool_field] = bool(validated[bool_field])
    
    # Validate listing type
    if 'listing_type' in validated:
        valid_types = {'daily', 'monthly', 'sale'}
        if validated['listing_type'] not in valid_types:
            logger.warning(f"Invalid listing_type: {validated['listing_type']}, defaulting to 'sale'")
            validated['listing_type'] = 'sale'
    
    # Validate text fields
    text_fields = [
        'title', 'description', 'address', 'location', 'district',
        'metro_station', 'contact_type', 'contact_phone', 'property_type',
        'source_url', 'source_website'
    ]
    for field in text_fields:
        if field in validated and validated[field]:
            if isinstance(validated[field], bytes):
                try:
                    validated[field] = validated[field].decode('utf-8')
                except UnicodeDecodeError:
                    validated[field] = None
            elif isinstance(validated[field], str):
                validated[field] = validated[field].strip()
                if not validated[field]:  # If empty after stripping
                    validated[field] = None
            else:
                validated[field] = None

    # Validate JSON fields
    json_fields = ['amenities', 'photos']
    for field in json_fields:
        if field in validated and validated[field]:
            if isinstance(validated[field], (list, dict)):
                try:
                    validated[field] = json.dumps(validated[field])
                except (TypeError, ValueError):
                    validated[field] = None
            elif isinstance(validated[field], str):
                try:
                    # Verify it's valid JSON by parsing and re-dumping
                    json_data = json.loads(validated[field])
                    validated[field] = json.dumps(json_data)
                except json.JSONDecodeError:
                    validated[field] = None
            else:
                validated[field] = None

    logger.debug(f"Validation completed for listing {validated.get('listing_id')}")
    return validated

def save_listings_to_db(connection, listings: List[Dict]) -> None:
    """Save listings to database with enhanced data validation and error handling"""
    logger = logging.getLogger(__name__)
    successful = 0
    failed = 0
    
    try:
        # Ensure connection is alive
        connection = ensure_connection(connection)
        cursor = connection.cursor(prepared=True)  # Use prepared statements
        
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
            ) AS new_listing
            ON DUPLICATE KEY UPDATE
                updated_at = VALUES(updated_at),
                price = VALUES(price),
                title = VALUES(title),
                description = VALUES(description),
                views_count = VALUES(views_count)
        """
        
        for listing in listings:
            try:
                # Validate and clean data
                sanitized = validate_listing_data(listing)
                
                if not sanitized:
                    logger.warning("Empty listing data after validation, skipping")
                    failed += 1
                    continue
                
                if not sanitized.get('listing_id'):
                    logger.warning("Missing listing_id, skipping")
                    failed += 1
                    continue
                
                # Prepare values in the correct order
                values = (
                    sanitized.get('listing_id'),
                    sanitized.get('title'),
                    sanitized.get('description'),
                    sanitized.get('metro_station'),
                    sanitized.get('district'),
                    sanitized.get('address'),
                    sanitized.get('location'),
                    sanitized.get('latitude'),
                    sanitized.get('longitude'),
                    sanitized.get('rooms'),
                    sanitized.get('area'),
                    sanitized.get('floor'),
                    sanitized.get('total_floors'),
                    sanitized.get('property_type'),
                    sanitized.get('listing_type'),
                    sanitized.get('price'),
                    sanitized.get('currency'),
                    sanitized.get('contact_type'),
                    sanitized.get('contact_phone'),
                    sanitized.get('whatsapp_available', False),
                    sanitized.get('views_count', 0),
                    sanitized.get('has_repair', False),
                    sanitized.get('amenities'),
                    sanitized.get('photos'),
                    sanitized.get('source_url'),
                    sanitized.get('source_website'),
                    sanitized.get('created_at', datetime.datetime.now()),
                    sanitized.get('updated_at', datetime.datetime.now()),
                    sanitized.get('listing_date')
                )
                
                # Execute with proper type handling
                cursor.execute(insert_query, values)
                successful += 1
                
                # Commit every 50 successful insertions
                if successful % 50 == 0:
                    connection.commit()
                    logger.debug(f"Committed batch of 50 listings. Total successful: {successful}")
                    
            except Exception as e:
                failed += 1
                logger.error(f"Error saving listing {listing.get('listing_id')}: {str(e)}")
                continue
        
        # Final commit for any remaining listings
        connection.commit()
        
        # Close cursor
        cursor.close()
        
        # Log results
        logger.info(f"Successfully saved {successful} listings")
        if failed > 0:
            logger.warning(f"Failed to save {failed} listings")
            
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise
    
async def run_scrapers():
    """Run all scrapers and aggregate results"""
    logger = logging.getLogger(__name__)
    all_results = []
    
    # Get configuration from environment
    load_dotenv()
    pages = int(os.getenv('SCRAPER_PAGES', 2))  # Default to 2 pages
    
    scrapers = [
        ("Arenda.az", OptimizedArendaScraper()),
        # ("EV10.az", EV10Scraper()),
        # ("YeniEmlak.az", YeniEmlakScraper()),
        # ("Emlak.az", EmlakAzScraper()),
        # ("Bina.az", BinaScraper())  
    ]
    
    logger.info(f"Starting scrapers with {pages} pages each")
    
    for name, scraper in scrapers:
        try:
            logger.info(f"Starting {name} scraper for {pages} pages")
            results = await scraper.run(pages=pages)
            if results:
                logger.info(f"{name} scraper completed: {len(results)} listings from {pages} pages")
                all_results.extend(results)
            else:
                logger.warning(f"{name} scraper completed but returned no results")
        except aiohttp.ClientError as e:
            logger.error(f"Network error in {name} scraper: {str(e)}", exc_info=True)
        except asyncio.TimeoutError:
            logger.error(f"Timeout error in {name} scraper", exc_info=True)
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