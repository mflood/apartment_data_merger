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
        raw_apt_name            text,
        raw_address             text,
        raw_city                text,
        raw_state               text,
        has_valid_address_parts boolean not null,
        num_available_units     numeric,
        address_type            int,
        load_datetime_string    text,
        load_datetime           timestamp,
        _create_date            timestamptz default now()
        )
    """

    build_db_conn().execute(sql)

    base_table_name = table_name.split('.')[-1]
    # index on zip
    sql = f"""
        create index {base_table_name}_zip_idx on {table_name} (zip)
    """
    build_db_conn().execute(sql)


def create_sqlserver_table(table_name):

    sql = f"""drop table if exists {table_name}"""
    build_db_conn().execute(sql)

    sql = f"""
    create table {table_name} (
        sqlserver_table_id      bigserial not null primary key,
        property_id             text,
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


    base_table_name = table_name.split('.')[-1]
    # index on zip
    sql = f"""
        create index {base_table_name}_zip_idx on {table_name} (zip)
    """
    build_db_conn().execute(sql)


def create_mapping_table(table_name):

    sql = f"""drop table if exists {table_name}"""
    build_db_conn().execute(sql)

    sql = f"""
    create table {table_name} (
        address_hash            text,
        property_id             text,
        address_similarity      numeric(8, 7),
        apt_name_similarity     numeric(8, 7),
        _create_date            timestamptz default now()
        )
    """

    build_db_conn().execute(sql)


create_snowflake_table(table_name=os.getenv("SNOWFLAKE_TABLE"))
create_sqlserver_table(table_name=os.getenv("SQLSERVER_TABLE"))
create_mapping_table(table_name=os.getenv("MAPPING_TABLE"))
