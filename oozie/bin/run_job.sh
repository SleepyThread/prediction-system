#!/bin/bash

script_dir=$(pwd)/$(dirname $0)
workflow_dir=$script_dir/../workflow
resources_dir=$script_dir/../resources

echo "replacing workflow dir in hdfs..."
hadoop fs -rmr /user/hadoop/workflow
hadoop fs -put $workflow_dir /user/hadoop

echo "submitting oozie job..."
oozie job --config $resources_dir/job.properties -run
