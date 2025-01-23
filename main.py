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
from scrapers.ipoteka import IpotekaScraper
from scrapers.unvan import UnvanScraper
from scrapers.vipemlak import VipEmlakScraper
from scrapers.lalafo import LalafoScraper
from scrapers.tap import TapAzScraper
from bright_data_proxy import BrightDataProxy
import mysql.connector
from mysql.connector import Error
import datetime
import tempfile
from typing import Dict, List, Optional, Tuple
from decimal import Decimal, ROUND_HALF_UP
import json
import re
import random
import time
from collections import defaultdict



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
    """Create database connection with proper character set and collation"""
    try:
        load_dotenv()
        
        logger = logging.getLogger(__name__)
        logger.info("Attempting database connection...")
        
        db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci',  # Using a more widely supported collation
            'use_unicode': True,
            'raise_on_warnings': True
        }
        
        connection = mysql.connector.connect(**db_config)
        
        # Set session variables for character set and collation
        cursor = connection.cursor()
        cursor.execute('SET NAMES utf8mb4')
        cursor.execute('SET CHARACTER SET utf8mb4')
        cursor.execute('SET character_set_connection=utf8mb4')
        cursor.close()
        
        logger.info("Successfully connected to database")
        return connection
        
    except Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
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

def validate_coordinates(lat: Optional[float], lon: Optional[float]) -> Tuple[Optional[str], Optional[str]]:
    """
    Validate coordinates and format them as strings matching DECIMAL(10,8).
    Returns tuple of (latitude, longitude) as strings or (None, None) if invalid.
    """
    logger = logging.getLogger(__name__)
    
    try:
        if lat is not None and lon is not None:
            lat_float = float(lat)
            lon_float = float(lon)
            
            # Check valid ranges
            if -90 <= lat_float <= 90 and -180 <= lon_float <= 180:
                # Format to exactly 8 decimal places as strings
                lat_str = "{:.8f}".format(lat_float)
                lon_str = "{:.8f}".format(lon_float)
                
                # Log values for debugging
                logger.debug(f"Formatted coordinates: lat={lat_str}, lon={lon_str}")
                
                return lat_str, lon_str
            else:
                logger.warning(f"Coordinates out of range: lat={lat_float}, lon={lon_float}")
        
        return None, None
        
    except (ValueError, TypeError) as e:
        logger.error(f"Error validating coordinates: {str(e)}")
        return None, None


def validate_listing_data(listing: Dict) -> Dict:
    """Validate and clean listing data before database insertion"""
    logger = logging.getLogger(__name__)
    validated = listing.copy()

    # Required fields validation
    if not validated.get('listing_id'):
        logger.error("Missing required listing_id")
        return {}

    # Text field length validation based on schema
    text_field_limits = {
        'title': 200,
        'metro_station': 100,
        'district': 100,
        'address': None,  # TEXT type
        'location': 200,
        'property_type': 50,
        'contact_type': 50,
        'contact_phone': 50,
        'source_url': None,  # TEXT type
        'source_website': 100,
        'currency': 10
    }

    # Truncate text fields to match database column lengths
    for field, max_length in text_field_limits.items():
        if field in validated and validated[field]:
            if isinstance(validated[field], bytes):
                try:
                    validated[field] = validated[field].decode('utf-8')
                except UnicodeDecodeError:
                    validated[field] = None
                    continue
                    
            if isinstance(validated[field], str):
                validated[field] = validated[field].strip()
                if max_length and len(validated[field]) > max_length:
                    logger.debug(f"Truncating {field} from {len(validated[field])} to {max_length} characters")
                    validated[field] = validated[field][:max_length]
                if not validated[field]:
                    validated[field] = None
            else:
                validated[field] = None

    # Validate numeric fields
    try:
        if 'price' in validated:
            price = float(validated['price'])
            if 0 < price < 1000000000:  # Reasonable price range
                validated['price'] = round(price, 2)
            else:
                validated.pop('price', None)
    except (ValueError, TypeError):
        validated.pop('price', None)

    try:
        if 'area' in validated:
            area = float(validated['area'])
            if 5 <= area <= 10000:  # Reasonable area range
                validated['area'] = round(area, 2)
            else:
                validated.pop('area', None)
    except (ValueError, TypeError):
        validated.pop('area', None)

    # Validate integer fields
    for int_field in ['rooms', 'floor', 'total_floors', 'views_count']:
        if int_field in validated:
            try:
                value = int(float(validated[int_field]))
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

    # Validate coordinates
    if 'latitude' in validated or 'longitude' in validated:
        try:
            lat = float(validated.get('latitude', 0))
            lon = float(validated.get('longitude', 0))
            
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                validated['latitude'] = round(lat, 8)
                validated['longitude'] = round(lon, 8)
            else:
                validated.pop('latitude', None)
                validated.pop('longitude', None)
        except (ValueError, TypeError):
            validated.pop('latitude', None)
            validated.pop('longitude', None)

    # Validate boolean fields
    for bool_field in ['whatsapp_available', 'has_repair']:
        if bool_field in validated:
            validated[bool_field] = bool(validated[bool_field])

    # Validate listing type
    if 'listing_type' not in validated or validated['listing_type'] not in ['daily', 'monthly', 'sale']:
        validated['listing_type'] = 'sale'  # default value

    # Validate JSON fields
    for json_field in ['amenities', 'photos']:
        if json_field in validated:
            try:
                if isinstance(validated[json_field], (list, dict)):
                    validated[json_field] = json.dumps(validated[json_field])
                elif isinstance(validated[json_field], str):
                    # Verify it's valid JSON
                    json.loads(validated[json_field])
                else:
                    validated[json_field] = None
            except (ValueError, TypeError, json.JSONDecodeError):
                validated[json_field] = None

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
        cursor = connection.cursor(prepared=True)
        
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
    """Run all scrapers and collect performance statistics"""
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    stats = {
        'success_count': defaultdict(int),
        'error_count': defaultdict(int),
        'error_details': defaultdict(lambda: defaultdict(int)),
        'duration': 0,
        'avg_time_per_listing': 0
    }
    
    all_results = []
    
    try:
        proxy_manager = BrightDataProxy()
        if not await proxy_manager.verify_proxy():
            logger.error("Failed to verify Bright Data proxy connection")
            return []
            
        scrapers = [
            # ("Arenda.az", OptimizedArendaScraper()),
            # ("EV10.az", EV10Scraper()),
            # ("YeniEmlak.az", YeniEmlakScraper()),
            # ("Emlak.az", EmlakAzScraper()),
            # ("Bina.az", BinaScraper()),
            # ("Ipoteka.az", IpotekaScraper()),
            # ("Unvan.az", UnvanScraper()),
            # ("VipEmlak.az", VipEmlakScraper()),
            # ("Lalafo.az", LalafoScraper()),
            ("Tap.az", TapAzScraper())
        ]
        
        for name, scraper in scrapers:
            try:
                scraper_start = time.time()
                logger.info(f"Starting {name} scraper")
                
                proxy_manager.apply_to_scraper(scraper)
                results = await scraper.run(pages=int(os.getenv('SCRAPER_PAGES', 2)))
                
                if results:
                    stats['success_count'][name] = len(results)
                    all_results.extend(results)
                
                scraper_duration = time.time() - scraper_start
                logger.info(f"{name} completed in {scraper_duration:.2f}s")
                
            except Exception as e:
                stats['error_count'][name] += 1
                error_type = type(e).__name__
                stats['error_details'][name][error_type] += 1
                logger.error(f"Error in {name}: {str(e)}", exc_info=True)
            
            await asyncio.sleep(random.uniform(2, 5))
    
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
    
    finally:
        total_duration = time.time() - start_time
        total_listings = len(all_results)
        
        stats['duration'] = total_duration
        stats['avg_time_per_listing'] = total_duration / total_listings if total_listings > 0 else 0
        
        # Send report via Telegram
        try:
            reporter = TelegramReporter()
            await reporter.send_report(stats)
        except Exception as e:
            logger.error(f"Failed to send Telegram report: {str(e)}")
        
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