use hadoop;

CREATE EXTERNAL TABLE vehicle(
id double,
name string,
state string,
interval double)
LOCATION '/user/hadoop/data/vehicle'
fields terminated by ','
lines terminated by '\n';

CREATE EXTERNAL TABLE usage(
id double,
rate double,
current_usage double,
capture_date string)
LOCATION '/user/hadoop/data/usage'
fields terminated by ','
lines terminated by '\n';



