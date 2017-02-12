#!/bin/bash

python america_producer.py 34.198.20.105 k1 &
python china_producer.py 34.198.20.105 k1 &
python singapore_producer.py 34.198.20.105 k1 &
python france_producer.py 34.198.20.105 k1 &


