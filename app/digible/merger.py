"""
    merger.py

    populates mapping_table
    by combining snowflake and sqlserver data
"""
import logging
from digible.loggingsetup import LOGNAME


# pylint: disable=too-few-public-methods
class Merger():
    """
        Knows how to create mapping table from snowflake table
        to sqlserver table
    """

    def __init__(self, db_conn, snowflake_table, sqlserver_table, mapping_table):
        self._logger = logging.getLogger(LOGNAME)
        self._db_conn = db_conn
        self._snowflake_table = snowflake_table
        self._sqlserver_table = sqlserver_table
        self._mapping_table = mapping_table

    def run_batches(self):
        """
            Breaks the job into a bunch of ranges of zip codes
        """
        batch_list = []
        for zipcode in range(100000):
            batch_list.append(f"{zipcode}".zfill(5))
            if len(batch_list) == 1000:
                self._run_batch(zip_code_list=batch_list)
                batch_list = []

        if batch_list:
            self._run_batch(zip_code_list=batch_list)

    def _run_batch(self, zip_code_list):
        """
            Processes a set of zip codes
        """
        self._logger.info("Merging %s - %s", zip_code_list[0], zip_code_list[-1])
        zips = [f"'{z}'" for z in zip_code_list]
        in_string = ', '.join(zips)

        sql = """
with base as (
    select
          digible_schema.similarity(k.address, q.address_line1) as address_similarity
        , digible_schema.similarity(k.apt_name, q.apt_name) as apt_similarity
        , row_number() over (partition by k.address_hash, q.property_id) as distinct_pairing
        , k.apt_name as k_apt_name
        , k.address
        , k.zip
        , q.apt_name as q_apt_name
        , q.address_line1
        , k.address_hash
        , q.property_id
     from {snowflake_table} k
    inner join {sqlserver_table} q
       on k.zip=q.zip
      and digible_schema.similarity(k.address, q.address_line1) > 0
    where k.zip IN ({in_string} )
),
matches as(
    select
          property_id
        , address_similarity
        , apt_similarity
        , address_similarity + apt_similarity as total_similarity
        , k_apt_name
        , address_hash
        , address
        , q_apt_name
        , address_line1
        , zip
        , row_number() over (partition by property_id order by address_similarity + apt_similarity desc) as match_rank
     from base
    where distinct_pairing = 1 order by 1, 2, 3
)
    insert into {mapping_table} (
          property_id
        , address_hash
        , address_similarity
        , apt_name_similarity
    ) select
          property_id
        , address_hash
        , address_similarity
        , apt_similarity
     from matches
    where match_rank = 1
      and total_similarity > 0.5;
      """.format(in_string=in_string,
                 snowflake_table=self._snowflake_table,
                 sqlserver_table=self._sqlserver_table,
                 mapping_table=self._mapping_table)

        self._db_conn.execute(sql)

# end
