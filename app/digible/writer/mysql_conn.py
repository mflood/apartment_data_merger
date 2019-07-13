"""
   buffered_writer.py

   Writes records to  table
"""
import logging
# pylint: disable=import-error
import pymysql
import pymysql.cursors
from digible.loggingsetup import LOGNAME

class MysqlConnException(Exception):
    """
        Exception to raise when things go South
    """
    pass

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments
class MysqlConn():
    """
        Manages Mysql Connection
        and executes statements
    """
    def __init__(self,
                 host,
                 port,
                 database,
                 username,
                 password):

        self._host = host
        self._port = int(port)
        self._database = database
        self._username = username
        self._password = password

        self._dry_run = False

        self._logger = logging.getLogger(LOGNAME)

    def set_dry_run(self, is_dry_run):
        """
            Turn on/off _dry_run
        """
        self._dry_run = is_dry_run

    def _get_connection(self):
        """
            Connects to mysql and returns the
            pymysql connection object
        """
        # Connect to the database
        try:
            connection = pymysql.connect(host=self._host,
                                         port=self._port,
                                         user=self._username,
                                         password=self._password,
                                         db=self._database,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)

            self._logger.debug("MysqlConn connected to %s %s", self._host, self._port)
            return connection

        except pymysql.err.OperationalError as error:
            message = f"Error connecting to mysql: {error}"
            self._logger.error(message)
            raise MysqlConnException(message)

    def execute(self, sql, param_list=None):
        """
            Utility method
            to execute DML
        """
        if not param_list:
            param_list = []

        if self._dry_run:
            self._logger.warning("DRY RUN - Skipping actual inserts")
            return

        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                self._logger.debug("Ececuting sql")
                cursor.execute(sql, param_list)
                cursor.execute("commit")

        except pymysql.err.InternalError as error:
            self._logger.error("SQL ERROR: %s", error)
            self._logger.error("Truncated SQL (2000 chars): %s", sql[:2000])
            raise MysqlConnException(error)
        finally:
            connection.close()

# end
