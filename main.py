# main.py file is the main entry point for the scraper application. It runs the scrapers, saves the results to the database, and sends a report to a Telegram channel.
import os
import logging
import asyncio
from pathlib import Path
from dotenv import load_dotenv
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
# from bright_data_proxy import BrightDataProxy
# from proxy_handler import ProxyHandler # 711_proxy
from proxy_handler import DataImpulseProxyHandler  # dataimpulse_proxy  
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
from telegram_reporter import TelegramReporter
from utils import generate_checksum
import pytz
from dataclasses import dataclass


scraper_configs = {
    'bina.az': {
        'class': OptimizedBinaScraper,
        'active_periods': [
            {'start': datetime.time(8, 0), 'end': datetime.time(15, 0), 'interval': 2},  # Peak hours every 2 min
            {'start': datetime.time(19, 0), 'end': datetime.time(3, 0), 'interval': 5},  # Evening every 5 min
            {'start': datetime.time(0, 0), 'end': datetime.time(4, 0), 'interval': 5},   # Night every 5 min
            {'start': datetime.time(6, 0), 'end': datetime.time(7, 0), 'interval': 30}   # Early morning every 30 min
        ],
        'pages': 1
    },
    'arenda.az': {
        'class': OptimizedArendaScraper,
        'active_periods': [
            {'start': datetime.time(0, 0), 'end': datetime.time(23, 59), 'interval': 15}  # Every 15 minutes

        ],
        'pages': 1
    },
    'tap.az': {
        'class': TapAzScraper,
        'active_periods': [
            {'start': datetime.time(0, 0), 'end': datetime.time(23, 59), 'interval': 23},  # Day time every 23 min
        ],
        'pages': 1
    },
    'emlak.az': {
        'class': EmlakAzScraper,
        'active_periods': [
            {'start': datetime.time(0, 0), 'end': datetime.time(23, 59), 'interval': 30}  # Every 30 minutes
        ],
        'pages': 1
    },
    'lalafo.az': {
        'class': LalafoScraper,
        'active_periods': [
            {'start': datetime.time(0, 0), 'end': datetime.time(23, 59), 'interval': 15}  # Every 15 min all day
        ],
        'pages': 1
    },
    'ev10.az': {
        'class': EV10Scraper,
        'active_periods': [
            {'start': datetime.time(0, 0), 'end': datetime.time(23, 59), 'interval': 720}  # Every 12 hours
        ],
        'pages': 1
    },
    'unvan.az': {
        'class': UnvanScraper,
        'active_periods': [
            {'start': datetime.time(0, 0), 'end': datetime.time(23, 59), 'interval': 720}
        ],
        'pages': 1
    },
    'yeniemlak.az': {
        'class': YeniEmlakScraper,
        'active_periods': [
            {'start': datetime.time(0, 0), 'end': datetime.time(23, 59), 'interval': 6}  # Every 6 minutes
        ],
        'pages': 1
    },
    'ipoteka.az': {
        'class': IpotekaScraper,
        'active_periods': [
            {'start': datetime.time(0, 0), 'end': datetime.time(9, 0), 'interval': 45}
        ],
        'pages': 1
    },
    'vipemlak.az': {
        'class': VipEmlakScraper,
        'active_periods': [
            {'start': datetime.time(0, 0), 'end': datetime.time(23, 59), 'interval': 480}  # Every 8 hours
        ],
        'pages': 1
    }
    }

@dataclass
class ScraperConfig:
    name: str
    scraper_class: type
    active_periods: List[Dict]
    pages: int = 1

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
            'collation': 'utf8mb4_unicode_ci',
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

# validate_numeric_field function is used to validate and convert numeric fields with enhanced precision handling.
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
                    logger.warning(f"Failed to decode {field} value, setting to None")
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

    # Validate numeric fields with range checks
    try:
        if 'price' in validated:
            price = float(validated['price'])
            if 0 < price < 1000000000:  # Reasonable price range
                validated['price'] = round(price, 2)
            else:
                logger.warning(f"Price {price} outside valid range, removing")
                validated.pop('price', None)
    except (ValueError, TypeError):
        validated.pop('price', None)

    try:
        if 'area' in validated:
            area = float(validated['area'])
            if 5 <= area <= 10000:  # Reasonable area range in m²
                validated['area'] = round(area, 2)
            else:
                logger.warning(f"Area {area} outside valid range, removing")
                validated.pop('area', None)
    except (ValueError, TypeError):
        validated.pop('area', None)

    # Validate integer fields with specific ranges
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
                    logger.warning(f"{int_field} value {value} outside valid range, removing")
                    validated.pop(int_field, None)
            except (ValueError, TypeError):
                logger.warning(f"Invalid {int_field} value, removing")
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
                logger.warning(f"Invalid coordinates: lat={lat}, lon={lon}, removing both")
                validated.pop('latitude', None)
                validated.pop('longitude', None)
        except (ValueError, TypeError):
            logger.warning("Error converting coordinates, removing both")
            validated.pop('latitude', None)
            validated.pop('longitude', None)

    # Validate boolean fields
    for bool_field in ['whatsapp_available', 'has_repair']:
        if bool_field in validated:
            validated[bool_field] = bool(validated[bool_field])

    # Validate listing type
    if 'listing_type' not in validated or validated['listing_type'] not in ['daily', 'monthly', 'sale']:
        validated['listing_type'] = 'sale'  # default value
        logger.debug(f"Setting default listing_type='sale' for listing {validated.get('listing_id')}")

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
                logger.warning(f"Invalid {json_field} JSON, setting to None")
                validated[json_field] = None

    # Set created_at timestamp
    if 'created_at' not in validated:
        validated['created_at'] = datetime.datetime.now()

    # Remove updated_at if present
    validated.pop('updated_at', None)

    # Handle listing_date
    if 'listing_date' in validated:
        try:
            if isinstance(validated['listing_date'], str):
                validated['listing_date'] = datetime.datetime.strptime(validated['listing_date'], '%Y-%m-%d').date()
            elif isinstance(validated['listing_date'], datetime.datetime):
                validated['listing_date'] = validated['listing_date'].date()
            elif not isinstance(validated['listing_date'], datetime.date):
                validated['listing_date'] = None
        except (ValueError, TypeError):
            logger.warning("Invalid listing_date format, setting to None")
            validated['listing_date'] = None

    return validated

def save_listings_to_db(connection, listings: List[Dict]) -> Dict:
    """Save listings to database - simple insert mode"""
    logger = logging.getLogger(__name__)
    stats = {
        'successful_inserts': 0,
        'failed': 0,
        'error_details': defaultdict(int),
        'website_stats': defaultdict(lambda: {
            'new': 0,
            'failed': 0,
            'total_processed': 0
        })
    }
    
    try:
        connection = ensure_connection(connection)
        cursor = connection.cursor(prepared=True)
        
        # Simple insert query without any duplicate handling
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
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s
            )
        """
        
        for listing in listings:
            try:
                sanitized = validate_listing_data(listing)
                if not sanitized or not sanitized.get('listing_id') or not sanitized.get('source_website'):
                    stats['failed'] += 1
                    stats['error_details']['invalid_data'] += 1
                    continue
                
                website = sanitized['source_website']
                stats['website_stats'][website]['total_processed'] += 1
                
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
                    website,
                    sanitized.get('created_at', datetime.datetime.now()),
                    sanitized.get('listing_date')
                )
                
                cursor.execute(insert_query, values)
                stats['successful_inserts'] += 1
                stats['website_stats'][website]['new'] += 1
                
                # Commit every 50 records
                if stats['successful_inserts'] % 50 == 0:
                    connection.commit()
                    
            except Exception as e:
                stats['failed'] += 1
                error_type = type(e).__name__
                stats['error_details'][error_type] += 1
                stats['website_stats'][website]['failed'] += 1
                logger.error(f"Error saving listing {listing.get('listing_id')}: {str(e)}")
                continue
        
        # Final commit
        connection.commit()
        cursor.close()
        
        return stats
        
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise

def get_current_interval(scraper_name: str, active_periods: List[Dict]) -> int:
    """Get the current interval based on time of day"""
    timezone = pytz.timezone('Asia/Baku')
    current_time = datetime.datetime.now(timezone).time()
    
    for period in active_periods:
        start = period['start']
        end = period['end']
        
        # Handle periods crossing midnight
        if end < start:
            if current_time >= start or current_time <= end:
                return period['interval']
        else:
            if start <= current_time <= end:
                return period['interval']
    
    return active_periods[0]['interval']  # Default to first period interval

async def run_single_scraper(name: str, config: dict, proxy_manager: DataImpulseProxyHandler, connection, reporter: TelegramReporter, overall_stats: dict):
    """Run a single scraper continuously with its configured interval"""
    logger = logging.getLogger(__name__)
    
    while True:
        try:
            # Get current interval for this scraper
            interval = get_current_interval(name, config['active_periods'])
            
            scraper_start = time.time()
            logger.info(f"Starting {name} scraper")
            
            # Initialize and run scraper
            scraper = config['class']()
            proxy_manager.apply_to_scraper(scraper)
            results = await scraper.run(pages=config['pages'])
            
            if results:
                # Update source_website for all results
                for result in results:
                    result['source_website'] = name
                
                # Save results to database
                db_stats = save_listings_to_db(connection, results)
                
                # Update overall statistics
                overall_stats['success_count'][name] = len(results)
                overall_stats['website_stats'][name].update(db_stats['website_stats'][name])
                
                # Calculate and log metrics
                scraper_duration = time.time() - scraper_start
                avg_time = scraper_duration / len(results) if results else 0
                logger.info(f"{name} completed in {scraper_duration:.2f}s (avg {avg_time:.2f}s per listing)")
                
                # Send individual website report
                website_stats = {
                    'success_count': {name: len(results)},
                    'error_count': overall_stats['error_count'],
                    'error_details': overall_stats['error_details'],
                    'duration': scraper_duration,
                    'avg_time_per_listing': avg_time
                }
                website_db_stats = {
                    'successful_inserts': db_stats['successful_inserts'],
                    'failed': db_stats['failed'],
                    'website_stats': {name: db_stats['website_stats'][name]}
                }
                await reporter.send_report(website_stats, website_db_stats)
            
        except Exception as e:
            overall_stats['error_count'][name] += 1
            error_type = type(e).__name__
            overall_stats['error_details'][name][error_type] += 1
            logger.error(f"Error in {name}: {str(e)}", exc_info=True)
        
        # Wait for the configured interval before next run
        await asyncio.sleep(interval * 60)

async def run_scrapers():
    """Run all scrapers concurrently"""
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    overall_stats = {
        'success_count': defaultdict(int),
        'error_count': defaultdict(int),
        'error_details': defaultdict(lambda: defaultdict(int)),
        'duration': 0,
        'avg_time_per_listing': 0,
        'website_stats': defaultdict(lambda: {
            'new': 0,
            'updated': 0,
            'failed': 0,
            'total_processed': 0
        })
    }

    connection = None
    tasks = []
    
    try:
        # Initialize database connection
        connection = get_db_connection()
        logger.info("Database connection established")
        
        # Initialize proxy manager
        proxy_manager = DataImpulseProxyHandler()
        if not await proxy_manager.verify_proxy():
            logger.error("Failed to verify proxy connection")
            return overall_stats
        reporter = TelegramReporter()
        
        # Create tasks for each scraper
        for name, config in scraper_configs.items():
            task = asyncio.create_task(
                run_single_scraper(
                    name, 
                    config, 
                    proxy_manager, 
                    connection, 
                    reporter, 
                    overall_stats
                )
            )
            tasks.append(task)
        
        # Wait for all scrapers to complete (they run indefinitely)
        await asyncio.gather(*tasks, return_exceptions=True)
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
    
    finally:
        if connection:
            connection.close()
        
        # Calculate final statistics
        total_duration = time.time() - start_time
        total_listings = sum(overall_stats['success_count'].values())
        
        overall_stats['duration'] = total_duration
        overall_stats['avg_time_per_listing'] = total_duration / total_listings if total_listings > 0 else 0
        
        return overall_stats
    
async def main():
    """Main async function to run scrapers"""
    logger = setup_logging()
    logger.info("Starting scraper application")
    
    try:
        # Run scrapers and get overall statistics
        overall_stats = await run_scrapers()
        
        # Send final summary report
        try:
            reporter = TelegramReporter()
            await reporter.send_report(
                scraper_stats=overall_stats,
                db_stats={'website_stats': overall_stats['website_stats']}
            )
            logger.info("Final summary report sent successfully")
        except Exception as e:
            logger.error(f"Failed to send final summary report: {str(e)}", exc_info=True)
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("Application shutting down")
        
if __name__ == "__main__":
    asyncio.run(main())