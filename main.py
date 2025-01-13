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
    cert_file = None
    try:
        load_dotenv()
        
        # Read certificate content and clean up any potential whitespace issues
        cert_content = os.getenv('SSL_CERT', '').strip()
        if not cert_content:
            raise ValueError("SSL certificate not found in environment variables")
            
        # Create a temporary file for the certificate
        cert_file = tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False)
        cert_file.write(cert_content)
        cert_file.flush()  # Ensure all data is written to disk
        os.chmod(cert_file.name, 0o600)  # Set proper file permissions
        
        db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'port': int(os.getenv('PORT', '27566')),
            'raise_on_warnings': True,
            'ssl_ca': cert_file.name,
            'ssl_verify_cert': True,
            'ssl_verify_identity': True  # Added this for additional security
        }
        
        connection = mysql.connector.connect(**db_config)
        logging.info("Successfully connected to database with SSL")
        return connection
        
    except Error as e:
        logging.error(f"Error connecting to database: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise
    finally:
        # Clean up the temporary file
        if cert_file:
            try:
                os.unlink(cert_file.name)
            except Exception as e:
                logging.warning(f"Failed to remove temporary certificate file: {e}")
                


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

    # Required fields validation
    if not validated.get('listing_id'):
        logger.error("Missing required listing_id")
        return {}

    # Ensure listing type is valid
    if 'listing_type' not in validated or validated['listing_type'] not in ['daily', 'monthly', 'sale']:
        validated['listing_type'] = 'sale'
        logger.debug(f"Setting default listing_type='sale' for listing {validated.get('listing_id')}")

    # Validate and format coordinates
    # DECIMAL(10,8) means max 10 digits total with 8 after decimal point
    # This means we can only store values between -99.99999999 and 99.99999999
    if 'latitude' in validated or 'longitude' in validated:
        try:
            lat = float(validated.get('latitude', 0))
            lon = float(validated.get('longitude', 0))
            
            # Validate coordinate ranges
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                # Format to exactly 8 decimal places to match schema
                validated['latitude'] = round(lat, 8)
                validated['longitude'] = round(lon, 8)
                
                # Additional check for decimal precision
                if abs(validated['latitude']) >= 100 or abs(validated['longitude']) >= 100:
                    logger.warning(f"Coordinates too large for schema precision, removing: "
                                 f"lat={validated['latitude']}, lon={validated['longitude']}")
                    validated.pop('latitude', None)
                    validated.pop('longitude', None)
            else:
                logger.warning(f"Invalid coordinates, removing: lat={lat}, lon={lon}")
                validated.pop('latitude', None)
                validated.pop('longitude', None)
        except (ValueError, TypeError):
            validated.pop('latitude', None)
            validated.pop('longitude', None)

    # Validate numeric fields
    # Price: DECIMAL(12,2)
    if 'price' in validated:
        try:
            price = float(validated['price'])
            if 0 < price < 1000000000:  # Reasonable price range
                validated['price'] = round(price, 2)
            else:
                validated.pop('price', None)
        except (ValueError, TypeError):
            validated.pop('price', None)

    # Area: DECIMAL(10,2)
    if 'area' in validated:
        try:
            area = float(validated['area'])
            if 5 <= area < 100000:  # Expanded but reasonable area range
                validated['area'] = round(area, 2)
                # Check if result exceeds schema precision
                if validated['area'] >= 100000:
                    logger.warning(f"Area too large for schema precision: {validated['area']}")
                    validated.pop('area', None)
            else:
                validated.pop('area', None)
        except (ValueError, TypeError):
            validated.pop('area', None)

    # Integer fields
    for int_field in ['rooms', 'floor', 'total_floors', 'views_count']:
        if int_field in validated:
            try:
                value = int(float(validated[int_field]))
                # Field-specific validation
                if int_field == 'rooms' and 1 <= value <= 50:
                    validated[int_field] = value
                elif int_field in ['floor', 'total_floors'] and 0 <= value <= 200:
                    validated[int_field] = value
                elif int_field == 'views_count' and value >= 0:
                    validated[int_field] = value
                else:
                    validated.pop(int_field, None)
            except (ValueError, TypeError):
                validated.pop(int_field, None)

    # Boolean fields
    for bool_field in ['whatsapp_available', 'has_repair']:
        if bool_field in validated:
            validated[bool_field] = bool(validated[bool_field])

    # Validate text fields
    text_fields = [
        'title', 'description', 'address', 'location', 'district',
        'metro_station', 'contact_type', 'contact_phone', 'property_type',
        'source_url', 'source_website'
    ]
    for field in text_fields:
        if field in validated:
            if isinstance(validated[field], bytes):
                try:
                    validated[field] = validated[field].decode('utf-8')
                except UnicodeDecodeError:
                    validated[field] = None
            elif isinstance(validated[field], str):
                validated[field] = validated[field].strip()
                if not validated[field]:
                    validated[field] = None
            else:
                validated[field] = None

    # Validate JSON fields
    json_fields = ['amenities', 'photos']
    for field in json_fields:
        if field in validated:
            try:
                if isinstance(validated[field], (list, dict)):
                    validated[field] = json.dumps(validated[field])
                elif isinstance(validated[field], str):
                    # Verify it's valid JSON by parsing and re-dumping
                    json_data = json.loads(validated[field])
                    validated[field] = json.dumps(json_data)
                else:
                    validated[field] = None
            except (TypeError, ValueError, json.JSONDecodeError):
                validated[field] = None

    # Validate dates
    if 'listing_date' in validated and not isinstance(validated['listing_date'], datetime.date):
        try:
            if isinstance(validated['listing_date'], str):
                validated['listing_date'] = datetime.datetime.strptime(
                    validated['listing_date'], '%Y-%m-%d'
                ).date()
            else:
                validated.pop('listing_date', None)
        except (ValueError, TypeError):
            validated.pop('listing_date', None)

    # Ensure timestamps are set
    now = datetime.datetime.now()
    if 'created_at' not in validated:
        validated['created_at'] = now
    if 'updated_at' not in validated:
        validated['updated_at'] = now

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