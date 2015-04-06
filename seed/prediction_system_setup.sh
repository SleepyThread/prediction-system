#!/bin/sh


hive -f create_table.hql
hive -f seed_data.hql
