import logging
import os
import pytest
from digible.writer.pg_conn import PgConn
from digible.writer.pg_conn import PgConnException
import digible.loggingsetup as loggingsetup

loggingsetup.init(loglevel=logging.DEBUG)


@pytest.fixture
def pg_conn():

    postgres = PgConn(host=os.getenv("POSTGRES_HOST"),
                      port=os.getenv("POSTGRES_PORT"),
                      database=os.getenv("POSTGRES_DATABASE"),
                      username=os.getenv("POSTGRES_USERNAME"),
                      password=os.getenv("POSTGRES_PASSWORD"))

    return postgres


@pytest.fixture
def mysql_failed_conn():

    postgres = PgConn(host="localhost",
                      port="3306",
                      database="test",
                      username="fake",
                      password="fake")

    return postgres


def test_usage(pg_conn):
    pg_conn.set_dry_run(is_dry_run=False)
    try:
        pg_conn.execute("SELECT NOW()")
    except PgConnException as error:
        pass


def test_failed_conn(mysql_failed_conn):
    mysql_failed_conn.set_dry_run(is_dry_run=False)
    try:
        mysql_failed_conn.execute("SELECT NOW()")
    except PgConnException as error:
        pass


def test_dry_run(mysql_failed_conn):
    mysql_failed_conn.set_dry_run(is_dry_run=True)
    mysql_failed_conn.execute("SELECT NOW()")


def test_bad_sql(pg_conn):

    pg_conn.set_dry_run(is_dry_run=False)
    try:
        pg_conn.execute("SELECT BAD_THING")
    except PgConnException as error:
        pass

# end
