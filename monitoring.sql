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