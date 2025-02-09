-- event.sql is a file that contains the event scheduler configuration and event creation SQL statements. The event scheduler is a MySQL feature that allows you to schedule and automate tasks within the database. In this case, we are creating an event to process raw property listings into clean data every 5 minutes.
-- First, ensure event scheduler is enabled
SET GLOBAL event_scheduler = ON;

-- Drop existing event if it exists to avoid errors
DROP EVENT IF EXISTS process_raw_listings_event;

-- Create the event with more detailed scheduling and error handling
CREATE EVENT IF NOT EXISTS process_raw_listings_event
ON SCHEDULE
    EVERY 5 MINUTE
    STARTS CURRENT_TIMESTAMP
    ON COMPLETION PRESERVE
    ENABLE
    COMMENT 'Process raw listings into clean data every 5 minutes'
DO
    BEGIN
        -- Add error handling wrapper
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            -- Log error if needed
            -- You could insert into an error log table here
            ROLLBACK;
        END;
        
        START TRANSACTION;
            CALL process_raw_listings();
        COMMIT;
    END;

-- Verify event was created
SHOW EVENTS
WHERE Db = DATABASE()
AND Name = 'process_raw_listings_event';