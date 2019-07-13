

import datetime
import logging
import os
import pytest
import digible.loggingsetup as loggingsetup
from digible.writer.pg_conn import PgConn
from digible.writer.buffered_writer import BufferedWriter
from digible.writer.buffered_writer import BufferedWriterException

loggingsetup.init(loglevel=logging.DEBUG)

@pytest.fixture
def db_conn():

    postgres = PgConn(host=os.getenv("POSTGRES_HOST"),
                      port=os.getenv("POSTGRES_PORT"),
                      database=os.getenv("POSTGRES_DATABASE"),
                      username=os.getenv("POSTGRES_USERNAME"),
                      password=os.getenv("POSTGRES_PASSWORD"))

    postgres.set_dry_run(is_dry_run=True)
    return postgres


def test_usage(db_conn):
    writer = BufferedWriter(db_conn=db_conn,
                            fq_output_table="test_table")

    writer.init_queue(batchsize=100)
    writer.set_fields_snowflake()
    writer.set_worker_id(0)

    # test processing empty queue
    writer.run_inserts()

    count = 0
    for x in range(1005):
        count += 1
        dox_id = x
        friven_id = x

        record = {
            "num_available_units": -10.0,
            'has_valid_address_parts': 1,
            "address_hash": b'\xd5c\xd4\xadM\xf8\xfd\xf5!\xd7U\xe6\xfe\xbb\x08\x8a',
            "apt_name": "Club at Highland Park",
            "address": "11402 Evans St, Omaha, NE 68164",
            "zip": "68164",
            "city": "Omaha",
            "state": "NE",
            "load_datetime_string": "2019-06-21 22:13:10.652224",
            "load_datetime": datetime.datetime(2019, 6, 21, 22, 13, 10, 652224),
        }

        writer.add_record(output_record=record)

    writer.run_inserts()
    print(count)


def test_bad_misconfigured_fields(db_conn):
    writer = BufferedWriter(db_conn=db_conn,
                            fq_output_table="test_table")

    writer.init_queue(batchsize=10)
    writer.set_fields_snowflake()

    record = {
            "apt_name": "Club at Highland Park",
            "address": "11402 Evans St, Omaha, NE 68164",
            "zip": "68164",
            "city": "Omaha",
            "state": "NE",
    }

    with pytest.raises(BufferedWriterException):
        writer.add_record(output_record=record)
        writer.run_inserts()

