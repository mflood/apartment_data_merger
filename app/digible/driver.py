"""
    driver.py

    load config / parse args
    and then perform one of the following:

    snowflake: parse snowflake_table.txt and load table
    sqlserver: parse sqlserver_table.txt and load table
    merge: create the mapping table
"""

import argparse
import logging
import os
import sys
from digible import loggingsetup
from digible.writer.pg_conn import PgConn
from digible.writer.buffered_writer import BufferedWriter
from digible.parser.snowflake_table_parser import SnowflakeTableParser
from digible.parser.sqlserver_table_parser import SqlServerTableParser
from digible.merger import Merger

def parse_args(argv=None):
    """
        Parse command line args
    """
    parser = argparse.ArgumentParser(description="Main Driver for Frivenmeld")

    parser.add_argument('-v',
                        action="store_true",
                        dest="verbose",
                        required=False,
                        help="Debug output")

    parser.add_argument('--dryrun',
                        action="store_true",
                        dest="dry_run",
                        required=False,
                        help="Don't connect or write to database")

    parser.add_argument("--snowflake",
                        dest="do_snowflake",
                        action="store_true",
                        required=False,
                        help="Parse snowflake file and load snowflake table")

    parser.add_argument("--sqlserver",
                        dest="do_sqlserver",
                        action="store_true",
                        required=False,
                        help="Parse sqlserver file and load sqlserver table")

    parser.add_argument("--merge",
                        dest="do_merge",
                        action="store_true",
                        required=False,
                        help="Create merge table")

    results = parser.parse_args(argv)
    return results


def build_db_conn(host, port, database, username, password):
    """
        Create a Db Connection object,
        in this case a Postgres PgConn
    """
    postgres = PgConn(host=host,
                      port=port,
                      database=database,
                      username=username,
                      password=password)

    return postgres


def build_snowflake_buffered_writer(db_conn, snowflake_table):
    """
        Utiliy method for creating and configuring
        a buffered writer for snowflake table
    """
    writer = BufferedWriter(db_conn=db_conn,
                            fq_output_table=snowflake_table)

    writer.init_queue(batchsize=50000)
    writer.set_fields_snowflake()
    writer.set_worker_id(0)

    return writer


def build_sqlserver_buffered_writer(db_conn, sqlserver_table):
    """
        Utiliy method for creating and configuring
        a buffered writer for sqlserver table
    """
    writer = BufferedWriter(db_conn=db_conn,
                            fq_output_table=sqlserver_table)

    writer.init_queue(batchsize=50000)
    writer.set_fields_sqlserver()
    writer.set_worker_id(0)

    return writer


def process_snowflake_file(filepath, snowflake_writer):
    """
        Read the snowflake_table.txt and add the
        records to postgres database table
    """
    snowflake_parser = SnowflakeTableParser()
    data = snowflake_parser.parse_file(filepath=filepath)
    for item in data:
        record = {
            "num_available_units": item['available_units'],
            'has_valid_address_parts': bool(item['valid_address_parts']),
            "address_hash": item['address_hash'],
            "raw_apt_name": item["raw_apt_name"],
            "raw_address": item["raw_address"],
            "raw_city": item["raw_city"],
            "raw_state": item["raw_state"],
            "apt_name": item["apt_name"],
            "address": item["address"],
            "city": item["city"],
            "state": item["state"],
            "zip": item["zip"],
            "load_datetime_string": item["date_string"],
            "load_datetime": item["date_object"],
        }
        snowflake_writer.add_record(output_record=record)

    snowflake_writer.run_inserts()

def process_sqlserver_file(filepath, sqlserver_writer):
    """
        Read the snowflake_table.txt and add the
        records to postgres database table
    """
    parser = SqlServerTableParser()
    data = parser.parse_file(filepath=filepath)
    for item in data:
        record = {
            "property_id": item['property_id'],
            "apt_name": item["apt_name"],
            "address_line1": item["address_line1"],
            "address_line2": item["address_line2"],
            "city": item["city"],
            "state": item["state"],
            "zip": item["zip"],
        }
        sqlserver_writer.add_record(output_record=record)

    sqlserver_writer.run_inserts()


def load_config():
    """
        Return a config dict of al the config
        variables we need.
    """

    config = {
        'postgres_host': os.getenv("POSTGRES_HOST"),
        'postgres_port': os.getenv("POSTGRES_PORT"),
        'postgres_database': os.getenv("POSTGRES_DATABASE"),
        'postgres_username': os.getenv("POSTGRES_USERNAME"),
        'postgres_password': os.getenv("POSTGRES_PASSWORD"),
        'snowflake_table': os.getenv("SNOWFLAKE_TABLE"),
        'sqlserver_table': os.getenv("SQLSERVER_TABLE"),
        'mapping_table': os.getenv("MAPPING_TABLE"),
    }

    # All values are required...
    for key, value in config.items():
        if not value:
            logging.getLogger(loggingsetup.LOGNAME).error("env var %s is not set", key)
            sys.exit(1)

    return config


def main():
    """
        Args / Config / invoke
    """

    arg_object = parse_args()

    if arg_object.verbose:
        loggingsetup.init(logging.DEBUG)
    else:
        loggingsetup.init(logging.INFO)

    config = load_config()

    db_conn = build_db_conn(host=config["postgres_host"],
                            port=config["postgres_port"],
                            database=config["postgres_database"],
                            username=config["postgres_username"],
                            password=config["postgres_password"])

    if arg_object.dry_run:
        db_conn.set_dry_run(True)

    if arg_object.do_snowflake:
        db_conn.execute(f"truncate table {config['snowflake_table']}")
        snowflake_writer = build_snowflake_buffered_writer(
            db_conn=db_conn,
            snowflake_table=config['snowflake_table'])

        process_snowflake_file(filepath="../data/snowflake_table.txt",
                               snowflake_writer=snowflake_writer)

    elif arg_object.do_sqlserver:
        db_conn.execute(f"truncate table {config['sqlserver_table']}")
        sqlserver_writer = build_sqlserver_buffered_writer(
            db_conn=db_conn,
            sqlserver_table=config['sqlserver_table'])

        process_sqlserver_file(filepath="../data/sqlserver_table.txt",
                               sqlserver_writer=sqlserver_writer)
    elif arg_object.do_merge:
        db_conn.execute(f"truncate table {config['mapping_table']}")
        merger = Merger(db_conn=db_conn,
                        snowflake_table=config['snowflake_table'],
                        sqlserver_table=config['sqlserver_table'],
                        mapping_table=config['mapping_table'])
        merger.run_batches()


if __name__ == "__main__":
    main()

# end
