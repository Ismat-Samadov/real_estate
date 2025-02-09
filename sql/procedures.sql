-- procedure.sql contains the stored procedures and events for processing raw property listings
-- Begin stored procedure definition
DELIMITER //

CREATE PROCEDURE process_raw_listings()
BEGIN
    -- Declare variables for cursor and error handling
    DECLARE done INT DEFAULT FALSE;        -- Loop control flag
    DECLARE v_id BIGINT;                   -- Current record ID
    DECLARE v_listing_id VARCHAR(50);      -- Current listing ID
    DECLARE v_source_url TEXT;             -- Current source URL
    DECLARE v_source_website VARCHAR(200);  -- Current website
    DECLARE v_price VARCHAR(100);          -- Current price for validation
    DECLARE v_error_message TEXT;          -- For storing error messages
    
    -- Define cursor for fetching unprocessed records
    DECLARE cur CURSOR FOR 
        SELECT id, listing_id, source_url, source_website, price 
        FROM raw_property_listings 
        WHERE processed = FALSE;
    
    -- Set up handlers for loop completion and errors
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        -- Capture the error message
        GET DIAGNOSTICS CONDITION 1 
            v_error_message = MESSAGE_TEXT;
        
        -- Update the record with error information
        UPDATE raw_property_listings 
        SET processed = TRUE,
            process_message = v_error_message,
            process_timestamp = NOW()
        WHERE id = v_id;
    END;

    -- Open the cursor
    OPEN cur;
    
    -- Begin processing loop
    read_loop: LOOP
        -- Get next record
        FETCH cur INTO v_id, v_listing_id, v_source_url, v_source_website, v_price;
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        -- Validate price format (must be numeric)
        IF v_price REGEXP '^[0-9]+(\.[0-9]+)?$' THEN
            -- Check for existing listing to avoid duplicates
            IF NOT EXISTS (
                SELECT 1 FROM properties 
                WHERE source_url = v_source_url 
                AND source_website = v_source_website
            ) THEN
                -- Insert validated data into main properties table
                INSERT INTO properties (
                    -- [Column list remains the same]
                )
                SELECT 
                    listing_id,
                    title,
                    description,
                    metro_station,
                    district,
                    address,
                    location,
                    -- Convert and validate coordinates
                    CAST(NULLIF(latitude, '') AS DECIMAL(10,8)),
                    CAST(NULLIF(longitude, '') AS DECIMAL(10,8)),
                    -- Clean and convert numeric fields
                    CAST(NULLIF(REGEXP_REPLACE(rooms, '[^0-9]', ''), '') AS SIGNED),
                    CAST(NULLIF(REGEXP_REPLACE(area, '[^0-9.]', ''), '') AS DECIMAL(10,2)),
                    CAST(NULLIF(REGEXP_REPLACE(floor, '[^0-9]', ''), '') AS SIGNED),
                    CAST(NULLIF(REGEXP_REPLACE(total_floors, '[^0-9]', ''), '') AS SIGNED),
                    -- Pass through text fields
                    property_type,
                    -- Validate listing type
                    CASE 
                        WHEN listing_type IN ('daily', 'monthly', 'sale') THEN listing_type 
                        ELSE 'sale' 
                    END,
                    -- Clean and convert price
                    CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9.]', ''), '') AS DECIMAL(12,2)),
                    NULLIF(currency, ''),
                    contact_type,
                    contact_phone,
                    -- Convert boolean fields
                    CASE WHEN LOWER(whatsapp_available) IN ('1', 'true', 'yes') THEN 1 ELSE 0 END,
                    CAST(NULLIF(REGEXP_REPLACE(views_count, '[^0-9]', ''), '') AS SIGNED),
                    CASE WHEN LOWER(has_repair) IN ('1', 'true', 'yes') THEN 1 ELSE 0 END,
                    -- Pass through JSON fields
                    amenities,
                    photos,
                    source_url,
                    source_website,
                    created_at,
                    -- Convert date string to proper format
                    STR_TO_DATE(listing_date, '%Y-%m-%d')
                FROM raw_property_listings
                WHERE id = v_id;
                
                -- Mark record as successfully processed
                UPDATE raw_property_listings 
                SET processed = TRUE,
                    process_message = 'Successfully processed',
                    process_timestamp = NOW()
                WHERE id = v_id;
            ELSE
                -- Mark as duplicate if listing already exists
                UPDATE raw_property_listings 
                SET processed = TRUE,
                    process_message = 'Duplicate listing',
                    process_timestamp = NOW()
                WHERE id = v_id;
            END IF;
        ELSE
            -- Mark record as failed if price is invalid
            UPDATE raw_property_listings 
            SET processed = TRUE,
                process_message = 'Invalid price format',
                process_timestamp = NOW()
            WHERE id = v_id;
        END IF;
    END LOOP;

    -- Clean up by closing the cursor
    CLOSE cur;
END //

DELIMITER ;

-- Create automated event to run the processor every 5 minutes
CREATE EVENT IF NOT EXISTS process_raw_listings_event
ON SCHEDULE EVERY 5 MINUTE
DO CALL process_raw_listings();

-- Enable the event scheduler
SET GLOBAL event_scheduler = ON;