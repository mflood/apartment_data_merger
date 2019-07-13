"""
   pg_conn.py

   Manages Postgres Connection
"""
import logging
# pylint: disable=import-error
import psycopg2
import psycopg2.extras
from digible.loggingsetup import LOGNAME

class PgConnException(Exception):
    """
        Exception to raise when things go South
    """
    pass

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments
class PgConn():
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
        self._connect_timeout_seconds = 30

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
            conn = psycopg2.connect(dbname=self._database,
                                    user=self._username,
                                    password=self._password,
                                    host=self._host,
                                    port=self._port,
                                    connect_timeout=self._connect_timeout_seconds)

            self._logger.debug("PgConn connected to %s %s", self._host, self._port)
            self._logger.debug(conn)
            return conn
        except psycopg2.OperationalError as error:
            message = f"Error connecting to postgres: {error}"
            self._logger.error(message)
            raise PgConnException(message)

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
                self._logger.debug("Executing sql")
                cursor.execute(sql, param_list)
                cursor.execute("commit")

        except psycopg2.ProgrammingError as error:
            self._logger.error("SQL ERROR: %s", error)
            self._logger.error("Truncated SQL (2000 chars): %s", sql[:2000])
            raise PgConnException(error)
        finally:
            connection.close()

# end
