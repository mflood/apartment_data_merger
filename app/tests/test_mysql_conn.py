import logging
import os
import pytest
from digible.writer.mysql_conn import MysqlConn
from digible.writer.mysql_conn import MysqlConnException
import digible.loggingsetup as loggingsetup

loggingsetup.init(loglevel=logging.DEBUG)

@pytest.fixture
def mysql_conn():

    mysql = MysqlConn(host=os.getenv("MYSQL_HOST"),
                      port=os.getenv("MYSQL_PORT"),
                      database=os.getenv("MYSQL_DATABASE"),
                      username=os.getenv("MYSQL_USERNAME"),
                      password=os.getenv("MYSQL_PASSWORD"))
    
    return mysql

@pytest.fixture
def mysql_failed_conn():

    mysql = MysqlConn(host="localhost",
                      port="3306",
                      database="test",
                      username="fake",
                      password="fake")
    
    return mysql

def test_usage(mysql_conn):
    mysql_conn.set_dry_run(is_dry_run=False)
    try:
        mysql_conn.execute("SELECT NOW()")
    except MysqlConnException as error:
        pass
        
def test_failed_conn(mysql_failed_conn):
    mysql_failed_conn.set_dry_run(is_dry_run=False)
    try:
        mysql_failed_conn.execute("SELECT NOW()")
    except MysqlConnException as error:
        pass
        
def test_dry_run(mysql_failed_conn):
    mysql_failed_conn.set_dry_run(is_dry_run=True)
    mysql_failed_conn.execute("SELECT NOW()")

def test_bad_sql(mysql_conn):

    mysql_conn.set_dry_run(is_dry_run=False)
    try:
        mysql_conn.execute("SELECT BAD_THING")
    except MysqlConnException as error:
        pass

# end
