#!/bin/sh 
date
source venv/bin/activate
python3 aptmerge/driver.py --snowflake &
python3 aptmerge/driver.py --sqlserver &

wait
python3 aptmerge/driver.py --merge
date
