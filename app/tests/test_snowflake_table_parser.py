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
        "address_hash": b'\x85\x08?^\xcaUz\xe7\x14\xf7\xd45\x9b\xa0\xb9\xe3',
        "available_units": 10.0,
        "raw_apt_name": "Club at Highland Park",
        "raw_address": "11402 Evans St, Omaha, NE 68164",
        "raw_city": "Omaha",
        "raw_state": "NE",
        "apt_name": "CLUB AT HIGHLAND PARK",
        "address": "11402 EVANS ST",
        "city": "OMAHA",
        "state": "NE",
        "zip": "68164",
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
    
    


