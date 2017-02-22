#!/bin/bash


# Only use for testing or simplicity,
# runs all producers in parallel, but
# on the same machine.


kafka_base_dir=$PWD/all_producers

$kafka_base_dir/america_producer.py &
$kafka_base_dir/china_producer.py &
$kafka_base_dir/france_producer.py &
$kafka_base_dir/singapore_producer.py &
$kafka_base_dir/taiwan_producer.py &
