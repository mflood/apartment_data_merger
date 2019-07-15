#!/bin/sh 
date
source venv/bin/activate
python3 digible/driver.py --snowflake &
python3 digible/driver.py --sqlserver &

wait
python3 digible/driver.py --merge
date
