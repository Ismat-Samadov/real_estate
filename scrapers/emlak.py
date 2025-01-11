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

class EmlakAzScraper:
    """Scraper for emlak.az"""
    
    BASE_URL = "https://emlak.az"
    SALES_URL = "https://emlak.az/elanlar/?ann_type=1&sort_type=0"
    RENTAL_URL = "https://emlak.az/elanlar/?ann_type=2&sort_type=0"
    
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
                'Accept-Language': 'en-US,en;q=0.9',
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
        
        self.logger.info(f"Attempting to fetch URL: {url}")
        start_time = time.time()
        
        for attempt in range(MAX_RETRIES):
            try:
                self.logger.info(f"Attempt {attempt + 1} of {MAX_RETRIES}")
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        content = await response.text()
                        elapsed = time.time() - start_time
                        self.logger.info(f"Successfully fetched content in {elapsed:.2f} seconds")
                        return content
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
        """Extract numeric value from text with validation"""
        if not text:
            return None
        try:
            # Remove everything except digits and decimal point
            clean_text = re.sub(r'[^\d.]', '', text)
            
            # Handle multiple decimal points
            if clean_text.count('.') > 1:
                parts = clean_text.split('.')
                clean_text = parts[0] + '.' + ''.join(parts[1:])
            
            value = float(clean_text)
            
            # Validate reasonable ranges for area
            if value > 10000:  # Unreasonably large area
                return None
                
            return value
        except (ValueError, TypeError):
            return None

    async def parse_listing_page(self, html: str, listing_type: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('div.ticket.clearfix'):
            try:
                # Extract listing URL and ID
                link_elem = listing.select_one('h6.title a')
                if not link_elem:
                    continue
                    
                listing_url = self.BASE_URL + link_elem['href']
                listing_id = re.search(r'/(\d+)-', listing_url).group(1)
                
                # Extract price
                price_elem = listing.select_one('p.price')
                price_text = price_elem.text.strip() if price_elem else None
                price = self.extract_number(price_text) if price_text else None
                
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'emlak.az',
                    'price': price,
                    'currency': 'AZN',
                    'listing_type': listing_type,
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
                'updated_at': datetime.datetime.now(),
                'source_website': 'emlak.az'
            }

            # Log full HTML for debugging if needed
            # self.logger.debug(f"Processing HTML for listing {listing_id}: {html[:1000]}...")
            
            try:
                # Extract title and code
                title_elem = soup.select_one('h1.title')
                if title_elem:
                    data['title'] = title_elem.text.strip()
                
                code_elem = soup.select_one('p.pull-right b')
                if code_elem:
                    extracted_id = code_elem.text.strip()
                    if extracted_id != listing_id:
                        self.logger.warning(f"Listing ID mismatch: URL has {listing_id}, page shows {extracted_id}")
                    data['listing_id'] = extracted_id
            except Exception as e:
                self.logger.error(f"Error extracting title/code for listing {listing_id}: {str(e)}")

            try:
                # Extract price information
                price_elem = soup.select_one('div.price span.m')
                if price_elem:
                    price_text = price_elem.text.strip()
                    price = self.extract_number(price_text)
                    if price:
                        if 0 < price < 1000000000:  # Reasonable price range
                            data['price'] = price
                            data['currency'] = 'AZN'
                        else:
                            self.logger.warning(f"Invalid price value for listing {listing_id}: {price}")
            except Exception as e:
                self.logger.error(f"Error extracting price for listing {listing_id}: {str(e)}")

            # Extract technical characteristics with validation
            try:
                tech_chars = soup.select('dl.technical-characteristics dd')
                for char in tech_chars:
                    label_elem = char.select_one('.label')
                    if not label_elem:
                        continue
                        
                    label = label_elem.text.strip().lower()
                    value = char.text.replace(label_elem.text, '').strip()

                    try:
                        if 'sahə' in label:
                            area = self.extract_number(value)
                            if area and 5 <= area <= 1000:  # More strict area validation
                                data['area'] = area
                            else:
                                self.logger.warning(f"Invalid area value for listing {listing_id}: {area}m²")
                        elif 'otaqların sayı' in label:
                            rooms = self.extract_number(value)
                            if rooms and 1 <= rooms <= 20:  # Reasonable room range
                                data['rooms'] = int(rooms)
                        elif 'yerləşdiyi mərtəbə' in label:
                            floor = self.extract_number(value)
                            if floor and 0 <= floor <= 50:  # Reasonable floor range
                                data['floor'] = int(floor)
                        elif 'mərtəbə sayı' in label:
                            total_floors = self.extract_number(value)
                            if total_floors and 1 <= total_floors <= 50:
                                data['total_floors'] = int(total_floors)
                    except Exception as e:
                        self.logger.error(f"Error processing field {label} for listing {listing_id}: {str(e)}")
                        continue

            except Exception as e:
                self.logger.error(f"Error processing technical characteristics for listing {listing_id}: {str(e)}")
                if 'təmiri' in label:
                    repair_text = value.lower()
                    data['has_repair'] = any(x in repair_text for x in ['təmirli', 'əla təmir', 'yaxşı təmir'])
                    data['repair_type'] = value
                elif 'əmlakın növü' in label:
                    property_type = value.lower()
                    if 'köhnə tikili' in property_type:
                        data['property_type'] = 'old'
                    elif 'yeni tikili' in property_type:
                        data['property_type'] = 'new'
                    elif 'villa' in property_type:
                        data['property_type'] = 'villa'
                    elif 'həyət evi' in property_type:
                        data['property_type'] = 'house'
                    elif 'obyekt' in property_type:
                        data['property_type'] = 'commercial'
                elif 'sənədin tipi' in label:
                    document_text = value.lower()
                    if 'çıxarış' in document_text or 'kupça' in document_text:
                        data['document_type'] = 'ownership'
                    elif 'müqavilə' in document_text:
                        data['document_type'] = 'contract'
                    data['document_description'] = value
            
            # Extract property details
            tech_chars = soup.select('dl.technical-characteristics dd')
            for char in tech_chars:
                label = char.select_one('.label')
                if not label:
                    continue
                    
                label_text = label.text.strip().lower()
                value_text = char.text.replace(label.text, '').strip()
                
                if 'sahə' in label_text:
                    area = self.extract_number(value_text)
                    # Only set area if it's a reasonable value
                    if area and 5 <= area <= 10000:  # Reasonable range for property area in m²
                        data['area'] = area
                    else:
                        self.logger.warning(f"Invalid area value found: {value_text} for listing {listing_id}")
                elif 'otaqların sayı' in label_text:
                    data['rooms'] = self.extract_number(value_text)
                elif 'yerləşdiyi mərtəbə' in label_text:
                    data['floor'] = self.extract_number(value_text)
                elif 'mərtəbə sayı' in label_text:
                    data['total_floors'] = self.extract_number(value_text)
                elif 'təmiri' in label_text:
                    data['has_repair'] = 'təmirli' in value_text.lower()
                elif 'əmlakın növü' in label_text:
                    if 'köhnə tikili' in value_text.lower():
                        data['property_type'] = 'old'
                    elif 'yeni tikili' in value_text.lower():
                        data['property_type'] = 'new'
                    elif 'villa' in value_text.lower():
                        data['property_type'] = 'villa'
                    elif 'həyət evi' in value_text.lower():
                        data['property_type'] = 'house'
            
            # Extract contact information
            contact_elem = soup.select_one('.seller-data .phone')
            if contact_elem:
                data['contact_phone'] = contact_elem.text.strip()
                
            contact_type = soup.select_one('.seller-data .name-seller span')
            if contact_type:
                data['contact_type'] = contact_type.text.strip('()')
            
            # Extract view count and listing date
            views_elem = soup.select_one('.views-count strong')
            if views_elem:
                data['views_count'] = self.extract_number(views_elem.text)
                
            date_elem = soup.select_one('.date strong')
            if date_elem:
                try:
                    data['listing_date'] = datetime.datetime.strptime(date_elem.text.strip(), '%d.%m.%Y').date()
                except ValueError:
                    pass
            
            # Extract photos
            photos = []
            photo_elems = soup.select('.img img')
            for photo in photo_elems:
                src = photo.get('src')
                if src and not src.endswith('load.gif'):
                    photos.append(self.BASE_URL + src)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    async def scrape_listing_type(self, base_url: str, listing_type: str, pages: int = 2) -> List[Dict]:
        """Scrape specified number of pages for a given listing type"""
        results = []
        
        for page in range(1, pages + 1):
            try:
                # Prepare URL with page number
                url = f"{base_url}&page={page}"
                
                # Fetch and parse listings page
                html = await self.get_page_content(url)
                listings = await self.parse_listing_page(html, listing_type)
                
                # Fetch and parse each listing detail
                for listing in listings:
                    try:
                        detail_html = await self.get_page_content(listing['source_url'])
                        detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                        results.append({**listing, **detail_data})
                    except Exception as e:
                        self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error processing page {page}: {str(e)}")
                continue
                
        return results

    async def run(self, pages: int = 2):
        """Run the scraper for both sales and rentals"""
        try:
            self.logger.info("Starting Emlak.az scraper")
            await self.init_session()
            
            all_results = []
            
            # Scrape sales listings
            self.logger.info("Scraping sales listings")
            sales_results = await self.scrape_listing_type(self.SALES_URL, 'sale', pages)
            all_results.extend(sales_results)
            
            # Scrape rental listings
            self.logger.info("Scraping rental listings")
            rental_results = await self.scrape_listing_type(self.RENTAL_URL, 'monthly', pages)
            all_results.extend(rental_results)
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            await self.close_session()