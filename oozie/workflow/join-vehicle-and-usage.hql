DROP TABLE vehicle_data;

CREATE EXTERNAL TABLE vehicle_data(
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
FROM vehicle vc
JOIN usage us
ON vc.id = us.id



