-- monitoring.sql contains queries for monitoring the data in the properties table
SELECT 
    source_website,
    COUNT(distinct listing_id ) as listing_count,
    MIN(created_at) as earliest_listing,
    MAX(created_at) as latest_listing,
    TIMESTAMPDIFF(HOUR, MAX(created_at), NOW()) as hours_since_last_scrape
FROM defaultdb.properties
GROUP BY source_website
ORDER BY MAX(created_at) DESC;


--  Check most recently updated listings in last hour
SELECT 
    listing_id,
    source_website,
    title,
    price,
    views_count,
    created_at,
    updated_at,
    TIMESTAMPDIFF(MINUTE, created_at, updated_at) as minutes_between_updates
FROM properties
WHERE updated_at >= NOW() - INTERVAL 1 HOUR
    AND updated_at != created_at  # This ensures we only see actual updates
ORDER BY updated_at DESC;


-- check duplicate listings

with t as(
SELECT 
    listing_id,
count(listing_id) as say
FROM remart_scraper.properties
WHERE source_website  = 'bina.az'
group by listing_id
HAVING count(listing_id) >1)

SELECT 
    id,
    a.listing_id,
    title,
    metro_station,
    district,
    address,
    location,
    latitude,
    longitude,
    rooms,
    area,
    floor,
    total_floors,
    property_type,
    listing_type,
    price,
    currency,
    contact_type,
    contact_phone,
    whatsapp_available,
    description,
    views_count,
    created_at,
    updated_at,
    listing_date,
    has_repair,
    amenities,
    photos,
    source_url,
    source_website
FROM 
    remart_scraper.properties a inner join t on a.listing_id = t.listing_id
WHERE 
    source_website = 'bina.az'
--     AND created_at BETWEEN '2025-02-02 17:10:00' AND '2025-02-02 17:17:09'
ORDER BY 
    a.listing_id DESC;