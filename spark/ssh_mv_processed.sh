#!/bin/bash

# Have to call this script with full path
# which you have to do in a cron anyway
script_dir="$(dirname "$0")"
echo $script_dir

ssh ubuntu@172.31.0.132 "bash /home/ubuntu/dirmv_processed_hdfs_files.sh"
