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

class TapAzScraper:
    """Scraper for tap.az real estate listings"""
    
    BASE_URL = "https://tap.az"
    LISTINGS_URL = "https://tap.az/elanlar/dasinmaz-emlak/menziller?keywords_source=typewritten"
    
    def __init__(self):
        """Initialize scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None

    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept': 'text/html, */*; q=0.01',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                'Accept-Encoding': 'gzip, deflate, br',
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'DNT': '1'
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

    async def get_page_content(self, url: str, cursor: Optional[str] = None) -> str:
        """Fetch page content with retry logic and anti-bot measures"""
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
        DELAY = int(os.getenv('REQUEST_DELAY', 1))
        
        params = {'cursor': cursor} if cursor else None
        
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

    def extract_area(self, text: str) -> Optional[float]:
        """Extract area value from text"""
        if not text:
            return None
        match = re.search(r'(\d+(?:\.\d+)?)\s*m²', text)
        if match:
            try:
                area = float(match.group(1))
                
                # Validate reasonable bounds
                if area < 5 or area > 10000:
                    self.logger.warning(f"Area value {area} m² outside reasonable bounds (5-10000)")
                    return None
                    
                # Round to 2 decimal places
                area = round(area, 2)
                
                # Ensure total digits don't exceed 10 (including decimal places)
                str_area = f"{area:.2f}".replace('.', '')
                if len(str_area) > 10:
                    self.logger.warning(f"Area value {area} exceeds maximum digits (10)")
                    return None
                    
                return area
                
            except (ValueError, TypeError) as e:
                self.logger.error(f"Error converting area value: {text} - {str(e)}")
                return None
        return None

    def extract_rooms(self, text: str) -> Optional[int]:
        """
        Extract number of rooms from text. Returns 0 if room count exceeds 20.
        
        Args:
            text (str): Text containing room information
            
        Returns:
            Optional[int]: Number of rooms, 0 if > 20 rooms, None if no valid number found
        """
        if not text:
            return None
            
        match = re.search(r'(\d+)-otaqlı', text)
        if match:
            try:
                rooms = int(match.group(1))
                if 1 <= rooms <= 20:  # Reasonable room range
                    return rooms
                elif rooms > 20:  # Handle cases with more than 20 rooms
                    return 0
            except (ValueError, TypeError):
                pass
                
        return None
    
    def extract_floor_info(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Extract floor information from text patterns like "Mərtəbə: 2/5" or "2/5 mərtəbə"
        
        Args:
            text (str): Text containing floor information
            
        Returns:
            Tuple of (current floor, total floors) if found, (None, None) otherwise
        """
        if not text:
            return None, None
            
        # Common floor patterns
        patterns = [
            r'mərtəbə:\s*(\d+)/(\d+)',  # Mərtəbə: 2/5
            r'(\d+)/(\d+)\s*mərtəbə',   # 2/5 mərtəbə
            r'mərtəbə\s*(\d+)/(\d+)',   # mərtəbə 2/5
            r'(\d+)-ci mərtəbə\/(\d+)', # 2-ci mərtəbə/5
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    current_floor = int(match.group(1))
                    total_floors = int(match.group(2))
                    
                    # Basic validation
                    if 0 <= current_floor <= 200 and 1 <= total_floors <= 200:
                        return current_floor, total_floors
                except (ValueError, IndexError):
                    pass
                    
        return None, None
    
    def extract_coordinates(self, html: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract coordinates from the map element in the HTML.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            Tuple of (latitude, longitude) if found, (None, None) otherwise
        """
        # For tap.az items, try different patterns to capture coordinates
        
        # First try to look for any explicit lat/lon in the page (most common in tap.az)
        patterns = [
            # Standard patterns from various map implementations
            r'lat="([^"]+)".*?lon="([^"]+)"',
            r'data-lat="([^"]+)".*?data-lng="([^"]+)"',
            r'data-lat="([^"]+)".*?data-lon="([^"]+)"',
            # Google maps patterns
            r'google_map.*?value="\(([\d.]+),\s*([\d.]+)\)"',
            r'center=([\d.]+),([\d.]+)',
            # Leaflet patterns
            r'L\.marker\(\[([\d.]+),\s*([\d.]+)\]\)',
            # General coordinate text patterns
            r'coordinates.*?([\d.]+),\s*([\d.]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if match:
                try:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    # Validate reasonable bounds for Azerbaijan
                    if 38.0 <= lat <= 42.0 and 44.5 <= lon <= 51.0:
                        return lat, lon
                except (ValueError, TypeError, IndexError):
                    pass
        
        # Additional pattern for google maps embed
        iframe_match = re.search(r'google\.com/maps/embed.*?q=([\d.]+),([\d.]+)', html)
        if iframe_match:
            try:
                lat = float(iframe_match.group(1))
                lon = float(iframe_match.group(2))
                if 38.0 <= lat <= 42.0 and 44.5 <= lon <= 51.0:
                    return lat, lon
            except (ValueError, TypeError):
                pass
        
        return None, None
    
    def extract_amenities(self, html: str) -> Optional[str]:
        """
        Extract amenities from the listing HTML.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            JSON string of amenities if found, None otherwise
        """
        soup = BeautifulSoup(html, 'lxml')
        amenities = []
        
        # Look for property details section
        for prop in soup.select('.product-properties__i'):
            label = prop.select_one('.product-properties__i-name')
            value = prop.select_one('.product-properties__i-value')
            
            if label and value:
                amenities.append(f"{label.text.strip()}: {value.text.strip()}")
        
        # Look for other amenity sections if available
        amenity_sections = soup.select('.amenities, .features, .property-features')
        for section in amenity_sections:
            for item in section.select('li, .item'):
                text = item.text.strip()
                if text and text not in amenities:
                    amenities.append(text)
        
        # Extract features from description
        desc_elem = soup.select_one('.product-description__content')
        if desc_elem:
            desc_text = desc_elem.text.strip()
            # Look for features marked with bullet points or dashes
            bullet_items = re.findall(r'[•\-\*]\s*([^\n•\-\*]+)', desc_text)
            for item in bullet_items:
                item_text = item.strip()
                if item_text and len(item_text) < 100 and item_text not in amenities:
                    amenities.append(item_text)
        
        if amenities:
            return json.dumps(amenities)
        return None

    def extract_partial_phone(html: str) -> str:
        """
        Extract the partially hidden phone number from tap.az HTML.
        
        Args:
            html: HTML content containing the phone number
            
        Returns:
            Partially visible phone number
        """
        import re
        
        # Pattern to look for the partially hidden phone number
        pattern = r'<span>\((\d+)\) (\d+)-(\d+)-●●</span>'
        match = re.search(pattern, html)
        
        if match:
            area_code = match.group(1)
            first_part = match.group(2)
            second_part = match.group(3)
            
            # Format the partial number with placeholder
            partial_number = f"({area_code}) {first_part}-{second_part}-??"
            return partial_number
        
        # Alternative pattern if the first one doesn't match
        alt_pattern = r'<span>[^<]*\((\d+)\)[^<]*(\d+)[^<]*(\d+)[^<]*●●[^<]*</span>'
        alt_match = re.search(alt_pattern, html)
        
        if alt_match:
            area_code = alt_match.group(1)
            first_part = alt_match.group(2)
            second_part = alt_match.group(3)
            
            # Format the partial number with placeholder
            partial_number = f"({area_code}) {first_part}-{second_part}-??"
            return partial_number
            
        return "No partial phone number found"

    async def get_phone_numbers(self, listing_id: str) -> List[str]:
        # Try to get full phone numbers using API
        phones = await self._get_full_phone_numbers(listing_id)
        
        # If that fails, fall back to partial number
        if not phones:
            detail_html = await self.get_page_content(f"https://tap.az/elanlar/{listing_id}")
            partial = self.extract_partial_phone_from_page(detail_html)
            if partial:
                return [partial]
        
        return phones

    def _is_phone_number(self, text: str) -> bool:
        """Check if a string looks like a phone number"""
        # Remove common formatting characters
        clean = re.sub(r'[\s\-\(\)\+]', '', text)
        # Check if it's all digits and has a reasonable length for a phone number
        return clean.isdigit() and 7 <= len(clean) <= 15

    def extract_district(self, html: str) -> Optional[str]:
        """
        Extract district information from the listing HTML.
        Only returns a district if it matches one in the approved Azerbaijan districts list.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            District name if found and validated, None otherwise
        """
        # List of valid Azerbaijan districts (lowercased for case-insensitive matching)
        valid_districts = [
            "ağdam", "ağdaş", "ağcabədi", "ağstafa", "ağsu", "astara", "babək", "balakən", 
            "bərdə", "beyləqan", "biləsuvar", "cəbrayıl", "cəlilabad", "culfa", "daşkəsən", 
            "füzuli", "gədəbəy", "goranboy", "göyçay", "göygöl", "hacıqabul", "xaçmaz", 
            "xızı", "xocalı", "xocavənd", "imişli", "ismayıllı", "kəlbəcər", "kəngərli", 
            "kürdəmir", "qəbələ", "qax", "qazax", "qobustan", "quba", "qubadlı", "qusar", 
            "laçın", "lənkəran", "lerik", "masallı", "neftçala", "oğuz", "ordubad", "saatlı", 
            "sabirabad", "sədərək", "salyan", "samux", "şabran", "şahbuz", "şəki", "şamaxı", 
            "şəmkir", "şərur", "şuşa", "siyəzən", "tərtər", "tovuz", "ucar", "yardımlı", 
            "yevlax", "zəngilan", "zaqatala", "zərdab", "binəqədi", "xətai", "xəzər", 
            "qaradağ", "nərimanov", "nəsimi", "nizami", "pirallahı", "sabunçu", "səbail", 
            "suraxanı", "yasamal"
        ]
        
        soup = BeautifulSoup(html, 'lxml')
        district_candidates = []
        
        # First, get all possible location information from the property details
        city = None
        location = None
        
        # Check product properties
        for prop in soup.select('.product-properties__i'):
            label = prop.select_one('.product-properties__i-name')
            value = prop.select_one('.product-properties__i-value')
            
            if not label or not value:
                continue
                
            label_text = label.text.strip().lower()
            value_text = value.text.strip()
            
            if 'şəhər' in label_text:
                city = value_text
                district_candidates.append(value_text)
            elif 'yerləşmə yeri' in label_text:
                location = value_text
                district_candidates.append(value_text)
        
        # Try to extract from title and description as well
        title_elem = soup.select_one('h1.product-title')
        if title_elem:
            district_candidates.append(title_elem.text.strip())
        
        desc_elem = soup.select_one('.product-description__content')
        if desc_elem:
            district_candidates.append(desc_elem.text.strip())
        
        # Process each candidate location to extract potential district names
        extracted_districts = []
        
        for text in district_candidates:
            if not text:
                continue
                
            # Try to match district with r. or rayon pattern
            district_patterns = [
                r'(\w+)\s*r\.',           # "Xəzər r."
                r'(\w+)\s*rayonu',        # "Xəzər rayonu"
                r'(\w+)\s*r-nu',          # "Xəzər r-nu"
                r'(\w+)\s*rayon'          # "Xəzər rayon"
            ]
            
            for pattern in district_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    district_name = match.group(1).strip().lower()
                    extracted_districts.append(district_name)
            
            # Also check for metro stations that match district names
            metro_patterns = [
                r'(\w+)\s*m\.',           # "Nizami m."
                r'(\w+)\s*metro',         # "Nizami metro"
                r'(\w+)\s*m/st'           # "Nizami m/st"
            ]
            
            for pattern in metro_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    district_name = match.group(1).strip().lower()
                    extracted_districts.append(district_name)
            
            # Also add raw words that might be districts
            words = re.split(r'[,\s.;:-]+', text.lower())
            extracted_districts.extend(words)
        
        # Now validate against the list of actual districts
        for candidate in extracted_districts:
            candidate = candidate.lower()
            if candidate in valid_districts:
                # Return with proper capitalization
                return candidate.capitalize()
        
        # Handle special metro cases that correspond to districts
        # if location:
        #     location_lower = location.lower()
        #     # Check if it's H.Aslanov which corresponds to Xəzər district
        #     if "aslanov" in location_lower:
        #         return "Xəzər"
        #     # Add more special cases as needed
        
        # No valid district found
        return None
    
    def extract_metro_station(self, html: str) -> Optional[str]:
        """
        Extract metro station information from the listing HTML.
        Only returns a metro station if it matches one in the approved Baku metro stations list.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            Metro station name if found and validated, None otherwise
        """
        # List of valid Baku metro stations (lowercased for case-insensitive matching)
        valid_metro_stations = [
            "20 yanvar", "28 may", "8 noyabr", "azadlıq prospekti", "avtovağzal", 
            "bakmil", "cəfər cabbarlı", "dərnəgül", "elmlər akademiyası", "əhmədli", 
            "gənclik", "həzi aslanov", "xalqlar dostluğu", "içərişəhər", "inşaatçılar", 
            "koroğlu", "qara qarayev", "memar əcəmi", "nəsimi", "nərimanov", 
            "neftçilər", "nizami", "sahil", "xətai", "xocəsən", "ulduz"
        ]
        
        # Normalized forms for special cases (e.g., shortened versions)
        normalized_stations = {
            "20 yanvar": ["20 yanvar", "20yanvar", "yanvar"],
            "28 may": ["28 may", "28may", "may"],
            "8 noyabr": ["8 noyabr", "8noyabr", "noyabr"],
            "həzi aslanov": ["həzi aslanov", "h.aslanov", "aslanov"],
            "xalqlar dostluğu": ["xalqlar dostluğu", "dostluğu"],
            "cəfər cabbarlı": ["cəfər cabbarlı", "cabbarlı"],
            "elmlər akademiyası": ["elmlər akademiyası", "akademiyası"],
            "memar əcəmi": ["memar əcəmi", "əcəmi"],
            "qara qarayev": ["qara qarayev", "qarayev"],
            "azadlıq prospekti": ["azadlıq prospekti", "azadlıq"]
        }
        
        # Mapping for various alternative forms to canonical names
        metro_mapping = {}
        for canonical, variations in normalized_stations.items():
            for variation in variations:
                metro_mapping[variation] = canonical
        
        # For stations not in the mapping, add a direct mapping to themselves
        for station in valid_metro_stations:
            if station not in metro_mapping:
                metro_mapping[station] = station
        
        soup = BeautifulSoup(html, 'lxml')
        metro_candidates = []
        
        # First, get all possible location information from the property details
        location = None
        
        # Check product properties
        for prop in soup.select('.product-properties__i'):
            label = prop.select_one('.product-properties__i-name')
            value = prop.select_one('.product-properties__i-value')
            
            if not label or not value:
                continue
                
            label_text = label.text.strip().lower()
            value_text = value.text.strip()
            
            if 'yerləşmə yeri' in label_text:
                location = value_text
                metro_candidates.append(value_text)
        
        # Try to extract from title and description as well
        title_elem = soup.select_one('h1.product-title')
        if title_elem:
            metro_candidates.append(title_elem.text.strip())
        
        desc_elem = soup.select_one('.product-description__content')
        if desc_elem:
            metro_candidates.append(desc_elem.text.strip())
        
        # Process each candidate location to extract potential metro station names
        extracted_stations = []
        
        for text in metro_candidates:
            if not text:
                continue
                
            # Try to match metro station with m. or metro pattern
            metro_patterns = [
                r'(\w+(?:\s+\w+)*)\s*m\.',           # "Nizami m."
                r'(\w+(?:\s+\w+)*)\s*metro',         # "Nizami metro"
                r'(\w+(?:\s+\w+)*)\s*m/st',          # "Nizami m/st"
                r'(\w+(?:\s+\w+)*)\s*metrosu',       # "Nizami metrosu"
                r'm\.\s*(\w+(?:\s+\w+)*)',           # "m. Nizami"
                r'metro\s*(\w+(?:\s+\w+)*)',         # "metro Nizami"
                r'(\w+(?:\s+\w+)*)\s*metro\s+stansiyası'  # "Nizami metro stansiyası"
            ]
            
            for pattern in metro_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    station_name = match.group(1).strip().lower()
                    extracted_stations.append(station_name)
                    
                    # Also try without spaces for compound names (e.g. "20Yanvar")
                    if ' ' in station_name:
                        extracted_stations.append(station_name.replace(' ', ''))
            
            # Also add raw words that might be metro stations
            words = re.split(r'[,\s.;:-]+', text.lower())
            # Only consider words that could be metro stations (e.g., proper names)
            potential_words = [w for w in words if len(w) > 3 and w[0].isalpha()]
            extracted_stations.extend(potential_words)
        
        # Add special case handling for metro stations commonly formatted with digits
        for text in metro_candidates:
            # Special pattern for "20 Yanvar", "28 May", etc.
            digit_patterns = [
                r'(\d+)\s*(\w+)',  # "20 Yanvar" or "28 May"
            ]
            
            for pattern in digit_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    digit = match.group(1)
                    name = match.group(2).lower()
                    
                    # Check common date-based metro stations
                    if digit == "20" and name in ["yanvar", "january"]:
                        extracted_stations.append("20 yanvar")
                    elif digit == "28" and name in ["may"]:
                        extracted_stations.append("28 may")
                    elif digit == "8" and name in ["noyabr", "november"]:
                        extracted_stations.append("8 noyabr")
        
        # Now validate against the mapping of metro stations
        for candidate in extracted_stations:
            # Try to find in metro_mapping (including variations)
            if candidate in metro_mapping:
                canonical = metro_mapping[candidate]
                return canonical.capitalize()
            
            # Try partial matching for longer station names
            for valid_name, canonical in metro_mapping.items():
                # Check if candidate is a substantial part of a valid station name
                # Only for longer station names (to avoid false matches with short names)
                if len(valid_name) > 5 and (valid_name in candidate or candidate in valid_name):
                    similarity = len(set(valid_name) & set(candidate)) / len(set(valid_name) | set(candidate))
                    if similarity > 0.7:  # Threshold for similarity
                        return canonical.capitalize()
        
        # Handle special cases from location text
        if location:
            location_lower = location.lower()
                    
        # No valid metro station found
        return None
    
    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('.products-i'):
            try:
                # Get listing URL and ID
                link = listing.select_one('a.products-link')
                if not link:
                    continue
                    
                listing_url = self.BASE_URL + link['href']
                listing_id = link['href'].split('/')[-1]
                
                # Extract price
                price_elem = listing.select_one('.price-val')
                price = self.extract_number(price_elem.text) if price_elem else None
                
                # Extract title and metadata
                title = listing.select_one('.products-name')
                title_text = title.text.strip() if title else None
                
                # Extract area and rooms
                area = None
                rooms = None
                
                # Try to extract area and rooms from both title and description
                for text in [title_text, listing.select_one('.products-description')]:
                    if text:
                        if area is None:
                            area = self.extract_area(text)
                        if rooms is None:
                            rooms = self.extract_rooms(text)
                
                # Extract location and date
                location_elem = listing.select_one('.products-created')
                if location_elem:
                    location_parts = location_elem.text.strip().split(', ')
                    location = location_parts[0] if len(location_parts) > 0 else None
                    
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'tap.az',
                    'title': title_text,
                    'price': price,
                    'currency': 'AZN',
                    'area': area,
                    'rooms': rooms,
                    'location': location,
                    'created_at': datetime.datetime.now()
                }
                
                # Extract listing type from URL
                if 'kiraye' in listing_url.lower():
                    listing_data['listing_type'] = 'daily' if 'gunluk' in listing_url.lower() else 'monthly'
                else:
                    listing_data['listing_type'] = 'sale'
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page and fetch additional data"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'tap.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract description
            desc_elem = soup.select_one('.product-description__content')
            if desc_elem:
                data['description'] = desc_elem.text.strip()
            
            # Extract property details
            for prop in soup.select('.product-properties__i'):
                label = prop.select_one('.product-properties__i-name')
                value = prop.select_one('.product-properties__i-value')
                
                if not label or not value:
                    continue
                    
                label_text = label.text.strip().lower()
                value_text = value.text.strip()
                
                if 'sahə' in label_text:
                    area = self.extract_area(value_text)
                    if area is not None:  # Only update if we got a valid area
                        data['area'] = area
                    else:
                        # Try to extract just the number if area extraction failed
                        try:
                            num = float(re.sub(r'[^\d.]', '', value_text))
                            if 5 <= num <= 10000:
                                data['area'] = round(num, 2)
                        except (ValueError, TypeError):
                            pass
                elif 'yerləşmə yeri' in label_text:
                    data['location'] = value_text
                    
                    # If location contains address-like information, update address field
                    if not any(x in value_text.lower() for x in ['metro', 'rayon', 'district']) and len(value_text) > 5:
                        data['address'] = value_text.strip()
                elif 'otaq sayı' in label_text:
                    try:
                        rooms = int(re.sub(r'[^\d]', '', value_text))
                        if 1 <= rooms <= 20:
                            data['rooms'] = rooms
                    except (ValueError, TypeError):
                        pass
                elif 'mərtəbə' in label_text:
                    # Extract floor information
                    floor_match = re.search(r'(\d+)/(\d+)', value_text)
                    if floor_match:
                        try:
                            floor = int(floor_match.group(1))
                            total_floors = int(floor_match.group(2))
                            if 0 <= floor <= 200 and 1 <= total_floors <= 200:
                                data['floor'] = floor
                                data['total_floors'] = total_floors
                        except (ValueError, IndexError):
                            pass
                elif 'elanın tipi' in label_text:
                    if 'kirayə' in value_text.lower():
                        data['listing_type'] = 'monthly'
                    elif 'satış' in value_text.lower():
                        data['listing_type'] = 'sale'
                elif 'binanın tipi' in label_text or 'əmlakın növü' in label_text:
                    if 'yeni tikili' in value_text.lower():
                        data['property_type'] = 'new'
                    elif 'köhnə tikili' in value_text.lower():
                        data['property_type'] = 'old'
                    elif 'həyət evi' in value_text.lower():
                        data['property_type'] = 'house'
                    elif 'mənzil' in value_text.lower():
                        data['property_type'] = 'apartment'
            
            # Extract district using the dedicated method that validates against allowed list
            district = self.extract_district(html)
            if district:
                data['district'] = district
                
            # Extract metro station using the dedicated method that validates against allowed list
            metro_station = self.extract_metro_station(html)
            if metro_station:
                data['metro_station'] = metro_station
            
            # Extract floor information if not already found
            if 'floor' not in data or 'total_floors' not in data:
                for info_elem in soup.select('.product-properties, .product-description__content'):
                    text = info_elem.text.strip().lower()
                    floor, total = self.extract_floor_info(text)
                    if floor is not None and 'floor' not in data:
                        data['floor'] = floor
                    if total is not None and 'total_floors' not in data:
                        data['total_floors'] = total
            
            # Extract amenities
            amenities = self.extract_amenities(html)
            if amenities:
                data['amenities'] = amenities
            
            # Extract coordinates
            lat, lon = self.extract_coordinates(html)
            if lat and lon:
                data['latitude'] = lat
                data['longitude'] = lon
            
            # Get phone numbers from API
            phones = await self.get_phone_numbers(listing_id)
            if phones:
                # Clean up phone number format
                phone = phones[0].replace('(', '').replace(')', '').replace('-', '').replace(' ', '')
                data['contact_phone'] = phone
            
            # Check WhatsApp availability
            whatsapp_elem = soup.select_one('.wp_status_ico')
            data['whatsapp_available'] = bool(whatsapp_elem)
            
            # Get seller info
            seller_info = soup.select_one('.product-owner__info-name')
            if seller_info:
                data['contact_type'] = seller_info.text.strip()
            
            # Extract photos
            photos = []
            photo_elems = soup.select('.product-photos__slider-top img')
            for img in photo_elems:
                src = img.get('src')
                if src and not src.endswith('load.gif'):
                    photos.append(src)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # Extract timestamps
            info_stats = soup.select('.product-info__statistics__i-text')
            for stat in info_stats:
                if 'Bugün' in stat.text:
                    data['listing_date'] = datetime.date.today()
                elif 'Baxışların sayı' in stat.text:
                    try:
                        views = int(re.search(r'\d+', stat.text).group())
                        data['views_count'] = views
                    except (ValueError, AttributeError):
                        pass
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    async def run(self, pages: int = 2) -> List[Dict]:
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting Tap.az scraper")
            await self.init_session()
            
            all_results = []
            cursor = None
            
            for page in range(pages):
                try:
                    # Fetch and parse listings page
                    html = await self.get_page_content(self.LISTINGS_URL, cursor)
                    listings = await self.parse_listing_page(html)
                    
                    # Update cursor for next page if available
                    cursor_match = re.search(r'cursor=([^"]+)', html)
                    if cursor_match:
                        cursor = cursor_match.group(1)
                    
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