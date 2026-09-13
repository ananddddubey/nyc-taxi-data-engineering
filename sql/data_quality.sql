-- Quality checks to run against fact_trips post-load

SELECT COUNT(*) AS null_trip_id FROM fact_trips WHERE trip_id IS NULL;

SELECT COUNT(*) AS negative_fares FROM fact_trips WHERE fare_amount < 0;

SELECT COUNT(*) AS zero_distance_trips FROM fact_trips WHERE trip_distance <= 0;

SELECT COUNT(*) AS orphaned_pickup_locations
FROM fact_trips f
LEFT JOIN dim_location l ON f.pickup_location_key = l.location_key
WHERE l.location_key IS NULL;

SELECT batch_id, status, COUNT(*)
FROM etl_audit
GROUP BY batch_id, status
ORDER BY batch_id;
