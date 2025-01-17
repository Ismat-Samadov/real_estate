SELECT 
    source_website,
    COUNT(distinct listing_id ) as listing_count,
    MIN(created_at) as earliest_listing,
    MAX(created_at) as latest_listing,
    TIMESTAMPDIFF(HOUR, MAX(created_at), NOW()) as hours_since_last_scrape
FROM defaultdb.properties
GROUP BY source_website
ORDER BY MAX(created_at) DESC;