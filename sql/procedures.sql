-- procedure.sql contains the stored procedures and events for processing raw property listings
-- Begin stored procedure definition
DELIMITER //

CREATE OR REPLACE PROCEDURE process_raw_listings()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_id BIGINT;
    DECLARE v_listing_id VARCHAR(50);
    DECLARE v_source_url TEXT;
    DECLARE v_source_website VARCHAR(200);
    DECLARE v_price VARCHAR(100);
    DECLARE v_checksum VARCHAR(64);
    DECLARE v_error_message TEXT;
    
    -- Cursor for unprocessed records
    DECLARE cur CURSOR FOR 
        SELECT id, listing_id, source_url, source_website, price, checksum
        FROM raw_property_listings 
        WHERE processed = FALSE;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        GET DIAGNOSTICS CONDITION 1 
            v_error_message = MESSAGE_TEXT;
        
        UPDATE raw_property_listings 
        SET processed = TRUE,
            process_message = v_error_message,
            process_timestamp = NOW()
        WHERE id = v_id;
    END;

    OPEN cur;
    
    read_loop: LOOP
        FETCH cur INTO v_id, v_listing_id, v_source_url, v_source_website, v_price, v_checksum;
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        -- Validate price (example validation)
        IF v_price REGEXP '^[0-9]+(\.[0-9]+)?$' THEN
            -- Check if listing exists and compare checksums
            IF EXISTS (
                SELECT 1 FROM properties 
                WHERE source_url = v_source_url 
                AND source_website = v_source_website
            ) THEN
                -- Update if checksum is different
                IF NOT EXISTS (
                    SELECT 1 FROM properties 
                    WHERE source_url = v_source_url 
                    AND checksum = v_checksum
                ) THEN
                    -- Update existing record with new data
                    UPDATE properties p
                    INNER JOIN raw_property_listings r ON r.id = v_id
                    SET 
                        p.price = CAST(NULLIF(REGEXP_REPLACE(r.price, '[^0-9.]', ''), '') AS DECIMAL(12,2)),
                        p.district = r.district,
                        p.checksum = r.checksum,
                        p.updated_at = NOW()
                    WHERE p.source_url = v_source_url;
                    
                    UPDATE raw_property_listings 
                    SET processed = TRUE,
                        process_message = 'Updated existing listing',
                        process_timestamp = NOW()
                    WHERE id = v_id;
                ELSE
                    -- Mark as processed but unchanged
                    UPDATE raw_property_listings 
                    SET processed = TRUE,
                        process_message = 'No changes detected',
                        process_timestamp = NOW()
                    WHERE id = v_id;
                END IF;
            ELSE
                -- Insert new record
                INSERT INTO properties (
                    listing_id, title, description, metro_station, district,
                    address, location, latitude, longitude, rooms, area, 
                    floor, total_floors, property_type, listing_type, price, 
                    currency, contact_type, contact_phone, whatsapp_available,
                    views_count, has_repair, amenities, photos,
                    source_url, source_website, created_at, listing_date, checksum
                )
                SELECT 
                    listing_id, title, description, metro_station, district,
                    address, location,
                    CAST(NULLIF(latitude, '') AS DECIMAL(10,8)),
                    CAST(NULLIF(longitude, '') AS DECIMAL(10,8)),
                    CAST(NULLIF(REGEXP_REPLACE(rooms, '[^0-9]', ''), '') AS SIGNED),
                    CAST(NULLIF(REGEXP_REPLACE(area, '[^0-9.]', ''), '') AS DECIMAL(10,2)),
                    CAST(NULLIF(REGEXP_REPLACE(floor, '[^0-9]', ''), '') AS SIGNED),
                    CAST(NULLIF(REGEXP_REPLACE(total_floors, '[^0-9]', ''), '') AS SIGNED),
                    property_type,
                    CASE 
                        WHEN listing_type IN ('daily', 'monthly', 'sale') THEN listing_type 
                        ELSE 'sale' 
                    END,
                    CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9.]', ''), '') AS DECIMAL(12,2)),
                    NULLIF(currency, ''),
                    contact_type,
                    contact_phone,
                    CASE WHEN LOWER(whatsapp_available) IN ('1', 'true', 'yes') THEN 1 ELSE 0 END,
                    CAST(NULLIF(REGEXP_REPLACE(views_count, '[^0-9]', ''), '') AS SIGNED),
                    CASE WHEN LOWER(has_repair) IN ('1', 'true', 'yes') THEN 1 ELSE 0 END,
                    amenities,
                    photos,
                    source_url,
                    source_website,
                    created_at,
                    STR_TO_DATE(listing_date, '%Y-%m-%d'),
                    checksum
                FROM raw_property_listings
                WHERE id = v_id;
                
                UPDATE raw_property_listings 
                SET processed = TRUE,
                    process_message = 'Successfully processed',
                    process_timestamp = NOW()
                WHERE id = v_id;
            END IF;
        ELSE
            -- Invalid price format
            UPDATE raw_property_listings 
            SET processed = TRUE,
                process_message = 'Invalid price format',
                process_timestamp = NOW()
            WHERE id = v_id;
        END IF;
    END LOOP;

    CLOSE cur;
END //

DELIMITER ;