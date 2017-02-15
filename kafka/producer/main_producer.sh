#!/bin/bash

kafka_base_dir=$PWD/all_producers


$kafka_base_dir/america_producer.py &
$kafka_base_dir/china_producer.py &
$kafka_base_dir/france_producer.py &
$kafka_base_dir/singapore_producer.py &
$kafka_base_dir/taiwan_producer.py &
