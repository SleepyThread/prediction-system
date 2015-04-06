use default;

show tables;

DROP TABLE IF EXISTS default.vehicle_data;

CREATE EXTERNAL TABLE default.vehicle_data(
id double,
interval double,
rate double,
current_usage double,
capture_date string)
LOCATION '${hivevar:location}';


insert overwrite directory '${hivevar:location}'
SELECT
vc.id,
vc.interval,
us.rate,
us.current_usage,
us.capture_date
FROM default.vehicle vc
JOIN default.usage us
ON vc.id = us.id



