CREATE TABLE properties (
    -- Primary key and unique identifier for each listing
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    listing_id VARCHAR(50) UNIQUE,         -- Adding UNIQUE directly to the column
    
    -- Basic property information
    title VARCHAR(200),                    -- Property title/headline
    metro_station VARCHAR(100),            -- Nearest metro station
    district VARCHAR(100),                 -- District/region name
    address TEXT,                          -- Full address
    location VARCHAR(200),                 -- General location description
    
    -- Geographic coordinates for mapping
    latitude DECIMAL(10, 8),               -- Latitude with 8 decimal precision
    longitude DECIMAL(10, 8),              -- Longitude with 8 decimal precision
    
    -- Property specifications
    rooms SMALLINT,                        -- Number of rooms
    area VARCHAR(200),                   -- Total area in square meters
    floor INT,                             -- Current floor of the property
    total_floors INT,                      -- Total number of floors in building
    
    -- Property categorization
    property_type VARCHAR(50),             -- Type of property
    listing_type ENUM('daily', 'monthly', 'sale'),
    
    -- Financial information
    price DECIMAL(12, 2),                  -- Price with 2 decimal precision
    currency VARCHAR(10),                  -- Currency code
    
    -- Contact information
    contact_type VARCHAR(50),              -- Type of contact
    contact_phone VARCHAR(50),             -- Contact phone number
    whatsapp_available BOOLEAN DEFAULT FALSE,
    
    -- Detailed information
    description TEXT,                      -- Full property description
    views_count INT DEFAULT 0,             -- Number of views/visits
    
    -- Timestamps and dates
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    listing_date DATE,                     -- Original listing date
    
    -- Additional features
    has_repair BOOLEAN DEFAULT FALSE,
    amenities TEXT,                        -- JSON string of amenities
    photos TEXT,                           -- JSON array of photo URLs
    
    -- Source tracking
    source_url TEXT,                       -- Original listing URL
    source_website VARCHAR(100),           -- Website where the listing was found
    
    INDEX idx_metro_station (metro_station),
    INDEX idx_district (district),
    INDEX idx_rooms (rooms),
    INDEX idx_price (price),
    INDEX idx_listing_type (listing_type),
    INDEX idx_property_type (property_type),
    INDEX idx_created_at (created_at),
    INDEX idx_listing_date (listing_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;