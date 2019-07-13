from collections import OrderedDict
import datetime
import logging
import pytest
from digible.parser.sqlserver_table_parser import SqlServerTableParser
import digible.loggingsetup as loggingsetup

loggingsetup.init(loglevel=logging.DEBUG)

@pytest.fixture()
def sqlserver_parser():
    stp = SqlServerTableParser()
    return stp

def test_parse_line(sqlserver_parser):
    
    # line 1
    line_dict = OrderedDict([('propertyId', '00da8697-2c7c-48fe-a653-5ee895d74a64'),
                             ('aptName', 'Antelope Ridge'),
                             ('addressCity', 'Colorado Springs'),
                             ('addressLine1', '4001 Gray Fox Heights'),
                             ('addressLine2', ''),
                             ('addressState', 'CO'),
                             ('addressZip', '80922')])

    result = sqlserver_parser.process_line(line_dict)
    expected = {
        "property_id": '00da8697-2c7c-48fe-a653-5ee895d74a64',
        "apt_name": "ANTELOPE RIDGE",
        "address_line1": "4001 GRAY FOX HEIGHTS",
        "address_line2": "",
        "city": "COLORADO SPRINGS",
        "state": "CO",
        "zip": "80922",
    }

    assert result == expected

def test_parse_file(sqlserver_parser):

    data = sqlserver_parser.parse_file(filepath="../data/sqlserver_table_sample.txt")
    for item in data:
        #logging.getLogger('digible').info("%s", item)
        pass
    
    


