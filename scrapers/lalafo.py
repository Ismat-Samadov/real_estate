import asyncio
import aiohttp
import logging
from typing import Dict, List, Optional, Tuple
import datetime
import json
import re

class LalafoScraper:
    """Scraper for lalafo.az using their API"""
    
    BASE_URL = "https://lalafo.az/api/search/v3/feed/search"
    
    def __init__(self):
        """Initialize the scraper"""
        self.logger = logging.getLogger(__name__)
        self.session = None

    async def init_session(self):
        """Initialize aiohttp session with required headers"""
        if not self.session:
            headers = {
                'authority': 'lalafo.az',
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                'country-id': '13',
                'device': 'pc',
                'language': 'az_AZ',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"'
            }
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(ssl=False)
            )

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    def extract_listing_type(self, ad_label: str) -> str:
        """Determine listing type from ad label"""
        label_lower = ad_label.lower() if ad_label else ''
        if 'günlük' in label_lower:
            return 'daily'
        elif 'kirayə' in label_lower or 'icarə' in label_lower:
            return 'monthly'
        return 'sale'

    def extract_property_type(self, title: str) -> str:
        """Determine property type from title/description"""
        title_lower = title.lower() if title else ''
        if 'villa' in title_lower:
            return 'villa'
        elif 'yeni tikili' in title_lower:
            return 'new'
        elif 'köhnə tikili' in title_lower:
            return 'old'
        elif 'həyət evi' in title_lower:
            return 'house'
        elif 'ofis' in title_lower:
            return 'office'
        elif 'obyekt' in title_lower:
            return 'commercial'
        return 'apartment'

    def extract_number(self, text: str) -> Optional[float]:
        """Extract numeric value from text"""
        if not text:
            return None
        try:
            return float(re.sub(r'[^\d.]', '', text))
        except:
            return None

    def validate_coordinates(self, lat: Optional[float], lng: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
        """Validate and format coordinates to match DECIMAL(10,8) schema constraints"""
        try:
            if lat is not None and lng is not None:
                lat_float = float(lat)
                lng_float = float(lng)
                
                # Check valid ranges
                if -90 <= lat_float <= 90 and -180 <= lng_float <= 180:
                    # Format to 8 decimal places to match schema DECIMAL(10,8)
                    return (
                        round(lat_float, 8),
                        round(lng_float, 8)
                    )
            return None, None
        except (ValueError, TypeError):
            return None, None

    def map_listing_to_schema(self, listing: Dict) -> Dict:
        """Map API response listing to database schema"""
        try:
            # Extract and validate coordinates
            lat, lng = self.validate_coordinates(
                listing.get('lat'), 
                listing.get('lng')
            )
            
            # Extract images
            photos = []
            if listing.get('images'):
                for img in listing['images']:
                    if img.get('original_url'):
                        photos.append(img['original_url'])

            # Map to schema
            mapped_data = {
                'listing_id': str(listing.get('id')),
                'title': listing.get('title'),
                'description': listing.get('description'),
                'metro_station': None,  # Need to extract from description/title
                'district': None,  # Need to extract from params if available
                'address': None,  # Not directly available
                'location': listing.get('city'),
                'latitude': lat,
                'longitude': lng,
                'rooms': None,  # Need to extract from title/description
                'area': None,  # Need to extract from title/description
                'floor': None,  # Need to extract from title/description
                'total_floors': None,  # Need to extract from title/description
                'property_type': self.extract_property_type(listing.get('title')),
                'listing_type': self.extract_listing_type(listing.get('ad_label')),
                'price': listing.get('price'),
                'currency': listing.get('currency', 'AZN'),
                'contact_type': None,  # Not directly available
                'contact_phone': listing.get('mobile'),
                'whatsapp_available': False,  # Not directly available
                'views_count': listing.get('views', 0),
                'has_repair': False,  # Need to extract from description
                'amenities': None,  # Need to extract from description
                'photos': json.dumps(photos) if photos else None,
                'source_url': f"https://lalafo.az{listing.get('url')}",
                'source_website': 'lalafo.az',
                'created_at': datetime.datetime.fromtimestamp(listing.get('created_time', 0)),
                'updated_at': datetime.datetime.fromtimestamp(listing.get('updated_time', 0)),
                'listing_date': datetime.datetime.fromtimestamp(listing.get('created_time', 0)).date()
            }

            # Extract district from params if available
            if listing.get('params'):
                for param in listing['params']:
                    if param.get('name') == 'İnzibati rayonlar':
                        mapped_data['district'] = param.get('value')

            # Extract additional info from title/description
            title_desc = f"{listing.get('title', '')} {listing.get('description', '')}"
            
            # Extract room count
            room_match = re.search(r'(\d+)\s*otaq', title_desc, re.IGNORECASE)
            if room_match:
                try:
                    rooms = int(room_match.group(1))
                    if 0 < rooms <= 50:  # Reasonable validation
                        mapped_data['rooms'] = rooms
                except ValueError:
                    pass

            # Extract area
            area_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m²|kv\.?\s*m)', title_desc, re.IGNORECASE)
            if area_match:
                try:
                    area = float(area_match.group(1))
                    if 5 <= area <= 1000:  # Reasonable validation
                        mapped_data['area'] = area
                except ValueError:
                    pass

            # Extract repair status
            if 'təmirli' in title_desc.lower():
                mapped_data['has_repair'] = True

            return mapped_data
            
        except Exception as e:
            self.logger.error(f"Error mapping listing {listing.get('id')}: {str(e)}")
            return {}

    async def fetch_page(self, page: int = 1, per_page: int = 20) -> List[Dict]:
        """Fetch a single page of listings from the API"""
        params = {
            'category_id': '2029',
            'expand': 'url',
            'page': str(page),
            'per-page': str(per_page),
            'with_feed_banner': 'true'
        }
        
        try:
            async with self.session.get(self.BASE_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('items', [])
                else:
                    self.logger.error(f"Error fetching page {page}: Status {response.status}")
                    return []
        except Exception as e:
            self.logger.error(f"Error fetching page {page}: {str(e)}")
            return []

    async def run(self, pages: int = 2) -> List[Dict]:
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting Lalafo.az scraper")
            await self.init_session()
            
            all_results = []
            
            for page in range(1, pages + 1):
                try:
                    self.logger.info(f"Fetching page {page}")
                    listings = await self.fetch_page(page)
                    
                    for listing in listings:
                        try:
                            mapped_data = self.map_listing_to_schema(listing)
                            if mapped_data:
                                all_results.append(mapped_data)
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing.get('id')}: {str(e)}")
                            continue
                            
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                
                # Add delay between pages
                await asyncio.sleep(1)
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            await self.close_session()