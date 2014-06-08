use ${hivevar:user};


CREATE EXTERNAL TABLE vehicle_data AS SELECT
vc.id,
vc.interval,
us.rate,
us.current_usage,
us.capture_date
FROM vehicle vc
JOIN usage us
ON vc.id = us.id
Location '${hivevar:user}'



