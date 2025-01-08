-- Main properties table to store real estate listings
CREATE TABLE properties (
    -- Primary key and unique identifier for each listing
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- Basic property information
    title VARCHAR(200),                    -- Property title/headline
    metro_station VARCHAR(100),            -- Nearest metro station
    district VARCHAR(100),                 -- District/region name
    address TEXT,                          -- Full address
    
    -- Geographic coordinates for mapping
    latitude DECIMAL(10, 8),               -- Latitude with 8 decimal precision
    longitude DECIMAL(10, 8),              -- Longitude with 8 decimal precision
    
    -- Property specifications
    rooms SMALLINT,                        -- Number of rooms (uses SMALLINT as it's rarely over 255)
    area DECIMAL(10, 2),                   -- Total area in square meters with 2 decimal precision
    floor INT,                             -- Current floor of the property
    total_floors INT,                      -- Total number of floors in building
    
    -- Property categorization
    property_type VARCHAR(50),             -- Type of property (e.g., apartment, house, villa)
    listing_type ENUM('daily', 'monthly', 'sale'),  -- Type of listing with restricted values
    
    -- Financial information
    price DECIMAL(10, 2),                  -- Price with 2 decimal precision (supports up to 99,999,999.99)
    currency VARCHAR(10),                  -- Currency code (e.g., AZN, USD)
    
    -- Contact information
    contact_type VARCHAR(50),              -- Type of contact (e.g., owner, agent, broker)
    contact_number VARCHAR(50),            -- Contact phone number
    whatsapp_available BOOLEAN,            -- Whether WhatsApp is available for contact
    
    -- Detailed information
    description TEXT,                      -- Full property description
    views_count INT,                       -- Number of views/visits to the listing
    listing_code VARCHAR(50),              -- Unique code assigned by the website
    
    -- Timestamps and dates
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When the record was created in our database
    updated_at DATETIME,                   -- When the record was last updated
    listing_date DATE,                     -- Original listing date on the website
    
    -- Additional features
    has_repair BOOLEAN,                    -- Whether the property is renovated
    amenities TEXT,                        -- JSON string of available amenities
    photos TEXT,                           -- JSON array of photo URLs
    
    -- Source tracking
    source_url TEXT,                       -- Original listing URL
    source_website VARCHAR(100)            -- Website where the listing was found
);

-- Create indexes for common search queries to improve performance
ALTER TABLE properties
    -- Index for searching by metro station
    ADD INDEX idx_metro_station (metro_station),
    
    -- Index for filtering by district
    ADD INDEX idx_district (district),
    
    -- Index for searching by number of rooms
    ADD INDEX idx_rooms (rooms),
    
    -- Index for price-based searches and sorting
    ADD INDEX idx_price (price),
    
    -- Index for filtering by listing type (daily/monthly/sale)
    ADD INDEX idx_listing_type (listing_type),
    
    -- Index for filtering by property type
    ADD INDEX idx_property_type (property_type);

/*
Additional notes:

1. Storage Requirements:
   - TEXT fields have no length limit but may impact performance
   - VARCHAR fields have specific length limits
   - DECIMAL(10,2) allows numbers up to 99,999,999.99

2. Field Choices:
   - ENUM for listing_type restricts values to prevent invalid data
   - BOOLEAN fields are used for yes/no flags
   - TIMESTAMP vs DATETIME: TIMESTAMP automatically converts to UTC

3. Indexing Strategy:
   - Indexes are added on frequently searched/filtered columns
   - TEXT fields are not indexed due to their length
   - Composite indexes could be added later based on query patterns

4. JSON Storage:
   - amenities and photos use TEXT to store JSON data
   - Consider migrating to JSON type if using MySQL 5.7+

5. Performance Considerations:
   - TEXT fields should be used sparingly
   - Indexes improve query performance but slow down writes
   - Consider partitioning for large datasets

6. Maintenance:
   - Regular ANALYZE TABLE recommended
   - Monitor index usage and remove unused indexes
   - Consider archiving old listings

7. Future Improvements:
   - Add foreign keys for normalized data (e.g., districts, metro_stations)
   - Add full-text search indexes for description
   - Add spatial index for coordinates
*/