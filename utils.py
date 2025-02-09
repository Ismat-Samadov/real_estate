# utils.py contains utility functions that are used in the main script
import hashlib
from typing import Dict

def generate_checksum(listing: Dict) -> str:
    """Generate a checksum based on price, source_url, and district to detect significant changes"""
    # Extract and clean price
    price = listing.get('price')
    if price is not None:
        try:
            # Normalize price to string with 2 decimal places
            price = "{:.2f}".format(float(price))
        except (ValueError, TypeError):
            price = str(price)
    else:
        price = "none"
    
    # Get source_url, default to empty string if not present
    source_url = str(listing.get('source_url', ''))
    
    # Get district, normalize it
    district = listing.get('district', '')
    if district:
        # Clean up district name (remove 'r.', normalize spaces)
        district = district.strip().lower()
        if 'r.' in district:
            district = district.replace('r.', '').strip()
    else:
        district = "none"
    
    # Combine values with a separator
    checksum_string = f"{price}|{source_url}|{district}"
    
    # Generate SHA-256 hash
    return hashlib.sha256(checksum_string.encode()).hexdigest()