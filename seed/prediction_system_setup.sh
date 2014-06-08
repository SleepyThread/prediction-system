#!/bin/sh


alias hive='/usr/local/share/hive/bin/hive'

hive -f create_table.hql
hive -f seed_data.hql
