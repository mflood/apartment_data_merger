"""
    snowflake_table_parser.py

    defines SnowflakeTableParser
"""
from datetime import datetime

import logging
import hashlib
import csv
import re
from digible.loggingsetup import LOGNAME


# used to patch states
ZIPCODE_STATES = {
    "99362": "WA",
    "52761": "IA",
    "10004": "NY",
    "97913": "OR",
    "79922": "TX",
    "89439": "NV",
    "56129": "MN",
    "82082": "WY",
    "38063": "TN",
    "30741": "GA",
    "48091": "MI",
    "97846": "OR",
    "88310": "NM",
    "38079": "TN",
    "42223": "KY",
    "60408": "IL",
    "72072": "AR",
    "77404": "TX",
}

class SnowflakeTableParser():
    """
        Parses snowflake_table.txt
        and loads database table
    """

    def __init__(self):
        self._logger = logging.getLogger(LOGNAME)
        self._city_only_regex = re.compile(r"([^,]*), ([A-Z][A-Z]) (\d+)")
        self._street_and_city_regex = re.compile(r"(.+), ([^,]*), ([A-Z][A-Z]) (\d+)")
        self._street_regex = re.compile(r"([-0-9]+[a-zA-Z]?) ([^,]+)")

    def make_id(self, apt_name, address): # pylint: disable=no-self-use
        """
            Create hash id from apt_name and address
        """
        concat = apt_name + "::" + address
        md5sum = hashlib.md5(concat.encode('utf8')).digest()
        return md5sum

    def verify_address_parts(self, address, city, state, zipcode): # pylint: disable=no-self-use
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
        except AssertionError:
            #self._logger.warning("Failed address validation: '{}' ({}, {} {})".format(
            #    address, city, state, zipcode))
            return 0

    def convert_address_to_parts(self, apt_name, address):
        """
            looks at apt_name and address
            to determine
            apt_name, address_line1, city, state, zipcode

            returns a dict.  If it can't determine
            a field, it omits it from the dict
        """
        city_only_match = self._city_only_regex.match(address)
        if city_only_match:
            return_dict = {
                "address_line1": "",
                "city": city_only_match.groups()[0],
                "state": city_only_match.groups()[1],
                "zipcode": city_only_match.groups()[2].zfill(5),
            }

            if self._street_regex.match(apt_name):
                return_dict['apt_name'] = ""
                return_dict["address_line1"] = apt_name
                #print(f"Address: {apt_name}")

            return return_dict

        street_and_city_match = self._street_and_city_regex.match(address)
        if street_and_city_match:
            return_dict = {
                "address_line1": street_and_city_match.groups()[0],
                "city": street_and_city_match.groups()[1],
                "state": street_and_city_match.groups()[2],
                "zipcode": street_and_city_match.groups()[3].zfill(5),
            }

            return return_dict

        if self._street_regex.match(address):
            return_dict = {
                "address_line1": address,
            }
            return return_dict


        return None

    def convert_string_to_date(self, datestring): # pylint: disable=no-self-use
        """
            given a date string like '2019-06-21 22:13:10.652224'
            return a datetime object
        """
        datetime_object = datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S.%f')
        return datetime_object


    def _strip_values(self, linedict): # pylint: disable=no-self-use
        """
            Strips and upper-cases address
            components.
            Makes zip-code 5 chars long
        """
        for key, value in linedict.items():
            linedict[key] = value.strip()

    def _fix_state(self, state, zipcode): # pylint: disable=no-self-use
        """
            There are some border towns...
        """
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

            city_part = parts.get("city", upper_city)
            state_part = parts.get("state", upper_state)
            zip_part = parts.get("zipcode", zip_five)
            fixed_address = parts.get("address_line1", upper_address)
            upper_apt_name = parts.get("apt_name", upper_apt_name)

            if (valid_address_parts
                    and city_part == upper_city
                    and state_part == fixed_state
                    and zip_part == zip_five):
                # everything matches
                pass
            elif (valid_address_parts
                  and city_part.endswith(upper_city)
                  and state_part == fixed_state
                  and zip_part == zip_five):
                # address has a more complete city name
                # than city "NEW HAVEN" vs "HAVEN"
                valid_address_parts = 0
            elif (not valid_address_parts
                  and city_part != upper_city
                  and state_part == fixed_state
                  and zip_part == zip_five):
                # print(f"ALTERNAME: {city_part} <=> {upper_city}")
                # when valid_address_parts is false,
                # we expect only the city to be different
                pass
            else:
                # These are cases that are not being
                # specifically handled yet
                #print("failed")
                #print(linedict)
                #print(f"'{city_part}' != '{upper_city}'")
                #print(f"'{state_part}' != '{fixed_state}'")
                #print(f"'{zip_part}' != '{zip_five}'")
                pass

        else:
            print(f"FAILED PARTS - APT NAME: {upper_apt_name} ADDRESS: {upper_address}")

        # if available_units is empty, make it None
        try:
            available_units = float(linedict['AVAILABLE_UNITS'])
        except ValueError:
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
