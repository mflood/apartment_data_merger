
from datetime import datetime

import logging
import hashlib
import csv
import re
from digible.loggingsetup import LOGNAME

class SnowflakeTableParser():

    def __init__(self):
        self._logger = logging.getLogger(LOGNAME)
        self._city_only_regex = re.compile("([^,]*), ([A-Z][A-Z]) (\d+)")
        self._street_regex = re.compile("([-0-9]+) ([^,]+)")

    def make_id(self, apt_name, address):
        """
            Create hash id from apt_name and address
        """
        concat = apt_name + "::" + address
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
            
    def convert_address_to_parts(self, apt_name, address):
        """

        """
        city_only_match = self._city_only_regex.match(address)
        if city_only_match:
            return_dict = {
                "address_type": 2,
                "address_line1": "",
                "city": city_only_match.groups()[0],
                "state": city_only_match.groups()[1],
                "zipcode": city_only_match.groups()[2].zfill(5),
            }

            if self._street_regex.match(apt_name):
                return_dict["address_line1"] = apt_name
                return_dict["address_type"] = 1
                #print(f"Address: {apt_name}")

            return return_dict
        else:
            return None

    def convert_string_to_date(self, datestring):
        """
            given a date string like '2019-06-21 22:13:10.652224'
            return a datetime object
        """
        datetime_object = datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S.%f')
        return datetime_object


    def _strip_values(self, linedict):
        """
            Strips and upper-cases address
            components.
            Makes zip-code 5 chars long
        """
        for key, value in linedict.items():
            linedict[key] = value.strip()

    def _fix_state(self, state, zipcode):
        """
            There are some border towns...
        """
        ZIPCODE_STATES = {
            "99362": "WA",
            "52761": "IA",
            "10004": "NY",
            "97913": "OR",
            "79922": "TX",
            "89439": "NV",
            "56129": "MN",
            "82082": "WY",
            "30741": "GA",
            "38063": "TN",
            "30741": "GA",
            "48091": "MI",
            "97846": "OR",
        }
        return ZIPCODE_STATES.get(zipcode, state)

    def process_line(self, linedict):
        """
            Given an OrderedDict from csvReader
            return a new dict with the processed data
        """

        self._strip_values(linedict=linedict)
        upper_apt_name = linedict['APT_NAME'].upper()
        upper_address = linedict['ADDRESS'].upper()
        upper_city = linedict['CITY'].upper()
        upper_state = linedict['STATE'].upper()
        zip_five = linedict['ZIP'].zfill(5).strip()

        fixed_state = self._fix_state(state=upper_state, zipcode=zip_five)

        address_hash = self.make_id(apt_name=upper_apt_name,
                                    address=upper_address)

        date_object = self.convert_string_to_date(datestring=linedict['DATE'])

        valid_address_parts = self.verify_address_parts(address=upper_address,
                                                        city=upper_city, 
                                                        state=fixed_state,
                                                        zipcode=zip_five)

        fixed_address = "" 
        parts = self.convert_address_to_parts(apt_name=upper_apt_name, address=upper_address)
        if parts:
            city_part = parts["city"]
            state_part = parts["state"]
            zip_part = parts["zipcode"]
            fixed_address = parts["address_line1"]

            if valid_address_parts and city_part == upper_city and state_part == fixed_state and zip_part == zip_five:
                pass
            elif valid_address_parts and city_part.endswith(upper_city) and state_part == fixed_state and zip_part == zip_five:
                valid_address_parts = 0
            elif not valid_address_parts and city_part != upper_city and state_part == fixed_state and zip_part == zip_five:
                #print(f"ALTERNAME: {city_part} <=> {upper_city}")
                pass
            else:
                print("failed")
                print(linedict)
                print(f"'{city_part}' != '{upper_city}'")
                print(f"'{state_part}' != '{fixed_state}'")
                print(f"'{zip_part}' != '{zip_five}'")


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
            'raw_apt_name': linedict['APT_NAME'],
            'raw_address': linedict['ADDRESS'],
            'raw_city': linedict['CITY'],
            'raw_state': linedict['STATE'],
            'apt_name': upper_apt_name,
            'address': fixed_address,
            'city': upper_city,
            'state': fixed_state,
            'zip': zip_five,
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

