DROP TABLE vehicle;
DROP TABLE usage;

CREATE EXTERNAL TABLE vehicle(
id double,
name string,
state string,
interval double)
ROW FORMAT DELIMITED
    fields terminated by ','
    lines terminated by '\n'
LOCATION '/user/hadoop/data/vehicle';

CREATE EXTERNAL TABLE usage(
id double,
rate double,
current_usage double,
capture_date string)
ROW FORMAT DELIMITED
    fields terminated by ','
    lines terminated by '\n'
LOCATION '/user/hadoop/data/usage';


CREATE EXTERNAL TABLE service_events(
id double,
servicingKm string,
servicingDate string
)
LOCATION '/user/hadoop/data/service_event';


