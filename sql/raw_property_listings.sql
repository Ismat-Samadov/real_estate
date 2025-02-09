-- raw_property_listings.sql contains the database schema for storing raw property listings
/*
This SQL script sets up a two-stage data processing system for real estate listings:
1. A staging table (raw_property_listings) that accepts unvalidated data
2. A processing procedure that validates and transfers data to the main properties table
3. An automated job that runs the processing regularly

Key features:
- No constraints on the staging table to ensure data ingestion
- Data validation and cleaning during processing
- Duplicate detection
- Error handling and logging
- Automated processing via event scheduler
*/

-- Create the staging table for incoming raw data
-- This table intentionally has minimal constraints to accept all data
CREATE TABLE raw_property_listings (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,  -- Unique identifier for each raw record
    listing_id VARCHAR(50),                -- Original listing ID from source
    title VARCHAR(500),                    -- Listing title, allowing longer text
    metro_station VARCHAR(200),            -- Nearby metro station
    district VARCHAR(200),                 -- District/region name
    address TEXT,                          -- Complete address
    location VARCHAR(500),                 -- General location description
    latitude VARCHAR(100),                 -- Stored as string initially for validation
    longitude VARCHAR(100),                -- Stored as string initially for validation
    rooms VARCHAR(50),                     -- Number of rooms (as string for validation)
    area VARCHAR(200),                     -- Property area (as string for validation)
    floor VARCHAR(50),                     -- Floor number (as string for validation)
    total_floors VARCHAR(50),              -- Total floors in building
    property_type VARCHAR(100),            -- Type of property
    listing_type VARCHAR(50),              -- Sale/Rent type
    price VARCHAR(100),                    -- Price (as string for validation)
    currency VARCHAR(50),                  -- Currency code
    contact_type VARCHAR(100),             -- Type of contact (agent/owner)
    contact_phone VARCHAR(100),            -- Contact phone number
    whatsapp_available VARCHAR(10),        -- WhatsApp availability flag
    description TEXT,                      -- Full property description
    views_count VARCHAR(50),               -- Number of listing views
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP NULL,             -- Last update timestamp
    listing_date VARCHAR(50),              -- Original listing date (for validation)
    has_repair VARCHAR(10),                -- Repair status flag
    amenities TEXT,                        -- JSON string of amenities
    photos TEXT,                           -- JSON array of photo URLs
    source_url TEXT,                       -- Original listing URL
    source_website VARCHAR(200),           -- Source website name
    checksum VARCHAR(64),                  -- For detecting changes in data
    processed BOOLEAN DEFAULT FALSE,        -- Processing status flag
    process_message TEXT,                  -- Processing result or error message
    process_timestamp TIMESTAMP            -- When the record was processed
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create indexes to optimize query performance
CREATE INDEX idx_raw_processed ON raw_property_listings(processed);  -- For finding unprocessed records
CREATE INDEX idx_raw_source_url ON raw_property_listings(source_url(255));  -- For duplicate checking
CREATE INDEX idx_raw_checksum ON raw_property_listings(checksum);    -- For change detection
