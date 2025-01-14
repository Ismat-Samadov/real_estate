import asyncio
import aiohttp
import random
import os
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional, Tuple
import datetime
import re
import json
import time

class VipEmlakScraper:
    """Scraper for vipemlak.az"""
    
    BASE_URL = "https://vipemlak.az"
    LISTINGS_URL = "https://vipemlak.az/yeni-tikili-satilir/"
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None
    
    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0'
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

    async def get_page_content(self, url: str, params: Optional[Dict] = None) -> str:
        """Fetch page content with retry logic and anti-bot measures"""
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
        DELAY = int(os.getenv('REQUEST_DELAY', 1))
        
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 403:
                        self.logger.warning(f"Access forbidden (403) on attempt {attempt + 1}")
                        await asyncio.sleep(DELAY * (attempt + 2))
                    else:
                        self.logger.warning(f"Failed to fetch {url}, status: {response.status}")
                        
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
            await asyncio.sleep(DELAY * (attempt + 1))
        
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

    def extract_number(self, text: str) -> Optional[float]:
        """Extract numeric value from text"""
        if not text:
            return None
        try:
            # Remove everything except digits and decimal point
            clean_text = re.sub(r'[^\d.]', '', text)
            return float(clean_text)
        except (ValueError, TypeError):
            return None

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('.pranto.prodbig'):
            try:
                # Get listing URL and ID
                link = listing.select_one('a')
                if not link:
                    continue
                    
                listing_url = self.BASE_URL + link.get('href', '')
                listing_id = re.search(r'-(\d+)\.html$', listing_url)
                if not listing_id:
                    continue
                listing_id = listing_id.group(1)
                
                # Extract title and parse rooms/district
                title = link.select_one('h3')
                title_text = title.text.strip() if title else None
                
                # Extract room count from title
                rooms = None
                rooms_match = re.search(r'(\d+)\s*otaq', title_text) if title_text else None
                if rooms_match:
                    rooms = int(rooms_match.group(1))
                
                # Extract district from title
                district = None
                district_match = re.search(r'(\w+)\s*rayonu', title_text) if title_text else None
                if district_match:
                    district = district_match.group(1)
                
                # Extract price
                price_elem = listing.select_one('.sprice')
                if price_elem:
                    price_text = price_elem.text.strip()
                    price = self.extract_number(price_text)
                
                # Extract description
                desc_elem = listing.select_one('.prodful')
                description = desc_elem.text.strip() if desc_elem else None
                
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'vipemlak.az',
                    'title': title_text,
                    'description': description,
                    'district': district,
                    'rooms': rooms,
                    'price': price,
                    'currency': 'AZN',
                    'listing_type': 'sale',
                    'created_at': datetime.datetime.now()
                }
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'vipemlak.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract main content div
            content_div = soup.select_one('.infotd100')
            if content_div:
                data['description'] = content_div.text.strip()
            
            # Extract property details
            for detail_row in soup.select('.infotd'):
                label = detail_row.text.strip().lower()
                value = detail_row.find_next('.infotd2')
                if not value:
                    continue
                    
                value_text = value.text.strip()
                
                if 'sahə' in label:
                    area_match = re.search(r'(\d+)\s*m²', value_text)
                    if area_match:
                        data['area'] = float(area_match.group(1))
                elif 'otaq sayı' in label:
                    rooms = self.extract_number(value_text)
                    if rooms:
                        data['rooms'] = int(rooms)
                elif 'qiymət' in label:
                    price = self.extract_number(value_text)
                    if price:
                        data['price'] = price
                        data['currency'] = 'AZN'
                elif 'əmlakın növü' in label:
                    property_type = value_text.lower()
                    if 'yeni tikili' in property_type:
                        data['property_type'] = 'new'
                    elif 'köhnə tikili' in property_type:
                        data['property_type'] = 'old'
            
            # Extract location info
            location_elem = soup.select_one('.infotd100 b:-soup-contains("Ünvan")')
            if location_elem:
                location_text = location_elem.parent.text
                
                # Extract metro station
                metro_match = re.search(r'(\w+)\s*metrosu', location_text)
                if metro_match:
                    data['metro_station'] = metro_match.group(1)
                
                # Extract district if not already found
                if not data.get('district'):
                    district_match = re.search(r'(\w+)\s*rayonu', location_text)
                    if district_match:
                        data['district'] = district_match.group(1)
                
                data['address'] = location_text
            
            # Extract contact info
            contact_elem = soup.select_one('.infocontact')
            if contact_elem:
                # Extract phone number
                phone_elem = contact_elem.select_one('#telshow')
                if phone_elem:
                    data['contact_phone'] = phone_elem.text.strip()
                
                # Extract contact type
                contact_type_elem = contact_elem.select_one('.glyphicon-user')
                if contact_type_elem and contact_type_elem.parent:
                    data['contact_type'] = 'agent' if 'vasitəçi' in contact_type_elem.parent.text.lower() else 'owner'
            
            # Extract photos
            photos = []
            photo_elems = soup.select('#picsopen img[src]')
            for img in photo_elems:
                src = img.get('src')
                if src and not src.endswith('load.gif'):
                    if not src.startswith('http'):
                        src = f"{self.BASE_URL}{src}"
                    photos.append(src)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # Extract listing date
            date_elem = soup.select_one('.viewsbb')
            if date_elem:
                try:
                    date_str = re.search(r'Tarix:\s*(\d{2}\.\d{2}\.\d{4})', date_elem.text).group(1)
                    data['listing_date'] = datetime.datetime.strptime(date_str, '%d.%m.%Y').date()
                except (ValueError, AttributeError):
                    pass
            
            # Extract floor information from description
            if data.get('description'):
                floor_match = re.search(r'(\d+)/(\d+)', data['description'])
                if floor_match:
                    try:
                        data['floor'] = int(floor_match.group(1))
                        data['total_floors'] = int(floor_match.group(2))
                    except ValueError:
                        pass
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    async def run(self, pages: int = 1):
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting VipEmlak scraper")
            await self.init_session()
            all_results = []
            
            for page in range(pages):
                try:
                    # Construct URL with page parameter
                    url = f"{self.LISTINGS_URL}?start={page * 5}"
                    
                    # Fetch and parse listings page
                    self.logger.info(f"Processing page {page + 1}")
                    html = await self.get_page_content(url)
                    listings = await self.parse_listing_page(html)
                    
                    # Fetch and parse each listing detail
                    for listing in listings:
                        try:
                            detail_html = await self.get_page_content(listing['source_url'])
                            detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                            all_results.append({**listing, **detail_data})
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                            continue
                            
                except Exception as e:
                    self.logger.error(f"Error processing page {page + 1}: {str(e)}")
                    continue
                    
                # Add delay between pages
                await asyncio.sleep(random.uniform(1, 2))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            await self.close_session()