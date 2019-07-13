
from datetime import datetime

import logging
import hashlib
import csv
from digible.loggingsetup import LOGNAME

class SnowflakeTableParser():

    def __init__(self):
        self._logger = logging.getLogger(LOGNAME)

    def make_id(self, apt_name, address):
        """
            Create hash id from apt_name and address
        """
        concat = apt_name.strip().upper() + "::" + address.strip().upper()
        md5sum = hashlib.md5(concat.encode('utf8')).digest()
        return md5sum

    def verify_address_parts(self, address, city, state, zipcode):
        """
            given this address:
            660 The Village, Redondo Beach, CA 90277
            
            We expect: 
                'zip': '90277'
                'city': ' REDONDO BEACH'
                'state': 'CA'

            This verifies that this holds true for a data point
        """
        expected_ending = "{}, {} {}".format(city, state, zipcode.zfill(5))
        try:
            assert address.upper().endswith(expected_ending.upper())
            return 1
        except AssertionError as error:
            #self._logger.warning("Failed address validation: '{}' ({}, {} {})".format(
            #    address, city, state, zipcode))
            return 0
            

    def convert_string_to_date(self, datestring):
        """
            given a date string like '2019-06-21 22:13:10.652224'
            return a datetime object
        """
        datetime_object = datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S.%f')
        return datetime_object

    def process_line(self, linedict):
        """
            Given an OrderedDict from csvReader
            return a new dict with the processed data
        """

        address_hash = self.make_id(apt_name=linedict['APT_NAME'],
                                    address=linedict['ADDRESS'])


        date_object = self.convert_string_to_date(datestring=linedict['DATE'])

        valid_address_parts = self.verify_address_parts(address=linedict['ADDRESS'],
                                                        city=linedict['CITY'], 
                                                        state=linedict['STATE'],
                                                        zipcode=linedict['ZIP'])

        # if available_units is empty, make it None
        try:
            available_units = float(linedict['AVAILABLE_UNITS'])
        except:
            available_units = None

        return_dict = {
            'address_hash': address_hash,
            'date_object': date_object,
            'available_units': available_units,
            'valid_address_parts': valid_address_parts,
            'apt_name': linedict['APT_NAME'].strip().upper(),
            'address': linedict['ADDRESS'].strip().upper(),
            'zip': linedict['ZIP'].zfill(5).strip().upper(),
            'city': linedict['CITY'].strip().upper(),
            'state': linedict['STATE'].strip().upper(),
            'date_string': linedict['DATE'],
        }

        return return_dict

    def parse_file(self, filepath):
        """
            Parse snowflake_table.txt
            yields a dictionary for each line
        """
        self._logger.info("Processing snowflake table file: %s", filepath)
        with open(filepath, "r") as handle:
            snowflake_reader = csv.DictReader(handle, delimiter="\x02")
            count = 0
            for row in snowflake_reader:
                #self._logger.debug("::".join(row.values()))
                data = self.process_line(linedict=row)
                #self._logger.debug(data)
                yield data
                count += 1
                #if count > 10:
                #    break
        self._logger.info("Processed %d items from %s", count, filepath)

# end

