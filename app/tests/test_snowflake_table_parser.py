import datetime
import logging
import pytest
from digible.parser.snowflake_table_parser import SnowflakeTableParser
import digible.loggingsetup as loggingsetup

loggingsetup.init(loglevel=logging.DEBUG)

@pytest.fixture()
def snowflake_parser():
    stp = SnowflakeTableParser()
    return stp

def test_parse_line(snowflake_parser):
    
    # line 1
    line_dict = {
            "AVAILABLE_UNITS": "10.0",
            "APT_NAME": "Club at Highland Park",
            "ADDRESS": "11402 Evans St, Omaha, NE 68164", 
            "ZIP": "68164",
            "CITY": "Omaha", 
            "STATE": "NE", 
            "DATE": "2019-06-21 22:13:10.652224"
    }

    result = snowflake_parser.process_line(line_dict)
    expected = {
        "address_hash": b'\xd5c\xd4\xadM\xf8\xfd\xf5!\xd7U\xe6\xfe\xbb\x08\x8a',
        "available_units": 10.0,
        "apt_name": "Club at Highland Park",
        "address": "11402 Evans St, Omaha, NE 68164",
        "zip": "68164",
        "city": "Omaha",
        "state": "NE",
        "date_string": "2019-06-21 22:13:10.652224",
        "date_object": datetime.datetime(2019, 6, 21, 22, 13, 10, 652224),
        'valid_address_parts': 1,
    }

    assert result == expected

def test_parse_file(snowflake_parser):

    data = snowflake_parser.parse_file(filepath="../data/snowflake_table_sample.txt")
    for item in data:
        #logging.getLogger('digible').info("%s", item)
        pass
    
    


