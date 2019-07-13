import logging
import os
import sys
import digible.loggingsetup as loggingsetup
from digible.writer.pg_conn import PgConn
from digible.writer.buffered_writer import BufferedWriter
from digible.parser.snowflake_table_parser import SnowflakeTableParser

def build_db_conn():
    postgres = PgConn(host=os.getenv("POSTGRES_HOST"),
                      port=os.getenv("POSTGRES_PORT"),
                      database=os.getenv("POSTGRES_DATABASE"),
                      username=os.getenv("POSTGRES_USERNAME"),
                      password=os.getenv("POSTGRES_PASSWORD"))

    return postgres


def create_snowflake_table(table_name):

    sql = f"""drop table if exists {table_name}"""
    build_db_conn().execute(sql)

    sql = f"""
    create table {table_name} (
        snowflake_table_id      bigserial not null primary key,
        address_hash            text, 
        apt_name                text,
        address                 text,
        city                    text,
        state                   text,
        zip                     text,
        has_valid_address_parts boolean not null,
        num_available_units     numeric,
        load_datetime_string    text,
        load_datetime           timestamp,
        _create_date            timestamptz default now()
        )
    """

    build_db_conn().execute(sql)

def create_sqlserver_table(table_name):

    sql = f"""drop table if exists {table_name}"""
    build_db_conn().execute(sql)

    sql = f"""
    create table {table_name} (
        sqlserver_table_id      bigserial not null primary key,
        apt_name                text,
        address_line1           text,
        address_line2           text,
        city                    text,
        state                   text,
        zip                     text,
        _create_date            timestamptz default now()
        )
    """

    build_db_conn().execute(sql)


create_snowflake_table(table_name=os.getenv("SNOWFLAKE_TABLE", "digible_schema.default_snowflake_table"))
create_sqlserver_table(table_name=os.getenv("SQLSERVER_TABLE", "digible_schema.default_sqlserver_table"))
