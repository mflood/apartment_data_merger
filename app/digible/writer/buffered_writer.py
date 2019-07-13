"""
   buffered_writer.py

   Writes records to  table
"""
import os
import logging
import queue
# pylint: disable=import-error
import pymysql
import pymysql.cursors
from digible.loggingsetup import LOGNAME

class BufferedWriterException(Exception):
    """
        Exception to raise when things go South
    """
    pass

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments
class BufferedWriter():
    """
        Writes records to database table
    """
    def __init__(self,
                 db_conn,
                 fq_output_table
                 ):

        self._db_conn = db_conn

        self._write_queue = None
        self._worker_id = 0
        self._fq_output_table = fq_output_table

        self._logger = logging.getLogger(LOGNAME)
        self._fields = []

    def set_fields_snowflake(self):
        """
            Sets the fields for the snowflake_table
        """

        self._fields = [
            "address_hash",
            "apt_name",
            "address",
            "city",
            "state",
            "zip",
            "load_datetime_string",
            "load_datetime",
            "has_valid_address_parts",
            "num_available_units",
        ]

    def set_worker_id(self, worker_id):
        """
            set the worker ID associated
            with records created

            use worker_id = 0 (default)
            if you are not using multiple workers
        """

        self._worker_id = int(worker_id)

        self._logger.info("BufferedWriter _worker_id set to %s", self._worker_id)

    def init_queue(self, batchsize):
        """
            Instantiates the queue,
            setting the maxsize to batchsize
        """
        assert batchsize > 0

        self._write_queue = queue.Queue(maxsize=batchsize)
        self._logger.info("BufferedWriter (%s) configured with queue size %s",
                          self._fq_output_table, batchsize)

    def add_record(self, output_record):
        """
            Add a record to the write queue
            if the queue is full (batchsize reached)
            then run inserts
        """

        try:
            self._write_queue.put(output_record, block=False)
        except queue.Full:
            self._logger.debug("BufferedWriter (%s) Queue is full. Running inserts.",
                               self._fq_output_table)
            self.run_inserts()
            # don't leave this guy out!
            self._write_queue.put(output_record, block=False)

    def run_inserts(self):
        """
            Reads the queue completely
            and creates a multi-insert sql statment
        """

        self._logger.info("BufferedWriter (%s) running inserts for all data in the queue.",
                          self._fq_output_table)

        # field1, field2, field3, etc....
        field_list = ", ".join(self._fields)

        # ['%s', '%s', '%s']
        interpolation_list = ["%s" for _ in range(len(self._fields))]

        # ( %s, %s, %s )
        interpolation_string = "(%s)" % ", ".join(interpolation_list)

        sql = "INSERT INTO %s (%s) values \n" % (self._fq_output_table, field_list)

        value_list = []
        param_list = []
        # build an insert statement
        # with multiple (e.g. 100) (value1, value2) insert things
        while True:
            try:
                output_record = self._write_queue.get(block=False)

                # add worker_id
                output_record["_worker_id"] = self._worker_id

                # (%s, %s, %s)
                value_list.append(interpolation_string)

                try:
                    # [ 3, 'john', 23 ]
                    arg_list = [output_record[fieldname] for fieldname in self._fields]
                    # ... '3', 'john', 23
                    param_list.extend(arg_list)
                except KeyError as error:

                    message = f"{self._fq_output_table} output field {error} is not in output_record: {output_record.keys()}"
                    self._logger.error(message)
                    self._logger.error("Configured Fields: %s", field_list)
                    self._logger.error("Row being processed: %s", output_record.keys())
                    raise BufferedWriterException(message)

            except queue.Empty:
                self._logger.debug("BufferedWriter (%s)  queue is empty.",
                                   self._fq_output_table)
                break

        if not value_list:
            # nothing in queue?
            return

        # (%s, %s),
        # (%s, %s),
        # (%s %s)
        sql += ",\n".join(value_list)

        self._db_conn.execute(sql, param_list)
        self._db_conn.execute("commit")

