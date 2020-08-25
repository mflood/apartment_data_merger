# Digible Coding Challenge

> This is my submission for the Digible Coding Challenge

## Overview

Processes apartment data from two disparate data sources and merges the data together:

* Create ETL to import data from two different sources
* Create data model to store data
* Create logic using heuristics to join the disparate data
* run reports
* Tests


## Download Data

> After downloading, move *snowflake_table.txt* and *sqlserver_table.txt* to *data/* directory

> [https://console.cloud.google.com/storage/browser/data_engineer_assessment](https://console.cloud.google.com/storage/browser/data_engineer_assessment)

## Initial Setup

### Set up python Environment

> NOTE: requires python3

```
cd app/
./setup.sh
```

### Set up Postgres database

> See [schema/README.md](schema/README.md)

### Set config values

> Update the variables in app/env\_secrets.sh

## Unit Testing

```
cd app
source envs_secret.sh 
./run_tests.sh
```

## Integration Testing

### Configure for sample data

> Update *env_secrets.sh* to use sample files instead of full files:

```
# app/envs_secret.sh

# ...

# sample set:
export SNOWFLAKE_FILE=../data/snowflake_table_sample.txt
export SQLSERVER_FILE=../data/sqlserver_table_sample.txt
```

#### Creating sample files

```
head -n 1000 data/snowflake_table.txt > data/snowflake_table_sample.txt
tail -n 1000 data/snowflake_table.txt >> data/snowflake_table_sample.txt

head -n 1000 data/sqlserver_table.txt > data/sqlserver_table_sample.txt
tail -n 1000 data/sqlserver_table.txt >> data/sqlserver_table_sample.txt
```

### Run the app

```
cd app/
source envs_secret.sh 
./run.sh
```

> See Explore Results to test exploration of data with sample


## Normal Run

### Configure for real data

> Update *env_secrets.sh* to use full files instead of sample files:

```
# app/envs_secret.sh

# ...

# sample set:
export SNOWFLAKE_FILE=../data/snowflake_table.txt
export SQLSERVER_FILE=../data/sqlserver_table.txt
```

### Run the app

```
cd app/
source envs_secret.sh 
./run.sh
```

## Explore results

```
cd schema/
localpg96 < queries.sql
```

> Copy output to plot.py

```
     0.6 |     4
     1.0 |     1
     1.1 |     1
     1.3 |     1
     1.4 |     1
     1.5 |     1
```

```
cd schema/
./run_plot.sh
```
