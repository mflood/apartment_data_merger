# Apartment Data Merger

An ETL pipeline that imports apartment listing data from two disparate
sources (a Snowflake export and a SQL Server export), merges the records
using matching heuristics, and loads the result into a Postgres data model
for reporting.

## Overview

* ETL to import data from two different source formats
* Postgres data model to store the merged data
* Heuristic matching logic to join records across the disparate sources
* Reporting queries and plots over the merged data
* Unit and integration tests

## Source data

The pipeline expects two exports in the `data/` directory:

* `snowflake_table.txt`
* `sqlserver_table.txt`

Small sample files (`*_sample.txt`) are included in `data/` so the pipeline
can be exercised without the full exports.

## Initial Setup

### Set up python environment

> NOTE: requires python3

```
cd app/
./setup.sh
```

### Set up Postgres database

> See [schema/README.md](schema/README.md)

### Set config values

> Update the variables in app/envs_secret.sh

## Unit Testing

```
cd app
source envs_secret.sh
./run_tests.sh
```

## Integration Testing

### Configure for sample data

> Update *envs_secret.sh* to use sample files instead of full files:

```
# app/envs_secret.sh

# ...

# sample set:
export SNOWFLAKE_FILE=../data/snowflake_table_sample.txt
export SQLSERVER_FILE=../data/sqlserver_table_sample.txt
```

#### Creating sample files from full exports

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

> Update *envs_secret.sh* to use full files instead of sample files:

```
# app/envs_secret.sh

# ...

# full set:
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
