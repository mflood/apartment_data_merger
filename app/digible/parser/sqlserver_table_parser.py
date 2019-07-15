"""
    sqlserver_table_parser.py

    defines SqlServerTableParser
"""
import logging
import hashlib
import csv
from digible.loggingsetup import LOGNAME

class SqlServerTableParser():
    """
        Parses SQLServer file
        and loads into database
    """

    def __init__(self):
        self._logger = logging.getLogger(LOGNAME)

    def make_id(self, apt_name, address): # pylint: disable=no-self-use
        """
            Create hash id from apt_name and address
        """
        concat = apt_name + "::" + address
        md5sum = hashlib.md5(concat.encode('utf8')).digest()
        return md5sum

    def process_line(self, linedict): # pylint: disable=no-self-use
        """
            Given an OrderedDict from csvReader
            return a new dict with the processed data
        """
        #self._logger.debug(linedict)
        return_dict = {
            'property_id': linedict['propertyId'].strip(),
            'apt_name': linedict['aptName'].strip().upper(),
            'address_line1': linedict['addressLine1'].strip().upper(),
            'address_line2': linedict['addressLine2'].strip().upper(),
            'city': linedict['addressCity'].strip().upper(),
            'state': linedict['addressState'].strip().upper(),
            'zip': linedict['addressZip'].zfill(5).strip(),
        }

        return return_dict

    def parse_file(self, filepath):
        """
            Parse sqlserver_table.txt
            yields a dictionary for each line
        """
        self._logger.info("Processing sqlserver table file: %s", filepath)
        with open(filepath, "r") as handle:
            # chr(164) is "¤"
            reader = csv.DictReader(handle, delimiter=chr(164))
            count = 0
            for row in reader:
                #self._logger.debug("::".join(row.values()))
                data = self.process_line(linedict=row)
                #self._logger.debug(data)
                yield data
                count += 1

        self._logger.info("Processed %d items from %s", count, filepath)

# end
