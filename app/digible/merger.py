"""
    merger.py

    populates mapping_table
    by combining snowflake and sqlserver data
"""
import logging
from digible.loggingsetup import LOGNAME

class Merger():

    def __init__(self, db_conn, output_table_name):
        self._logger = logging.getLogger(LOGNAME)
        self._db_conn = db_conn
        self._table_name = output_table_name

    def run_batches(self):
        """
            Breaks the job into a bunch of ranges of zip codes
        """
        self._run_batch(["19991", "18082", "18840"])

    def _run_batch(self, zip_code_list):
        """
            Processes a set of zip codes
        """

        zips = [f'"{z}"' for z in zip_code_list]
        in_string = ', '.join(zips)

        sql = """
with base as (
    select
          similarity(k.address, q.address_line1) as address_similarity
        , similarity(k.apt_name, q.apt_name) as apt_similarity
        , row_number() over (partition by k.address_hash, q.property_id) as distinct_pairing
        , k.apt_name as k_apt_name
        , k.address
        , k.zip,
        , q.apt_name as q_apt_name
        , q.address_line1
        , k.address_hash
        , q.property_id
     from snowflake_table k
    inner join sqlserver_table q
       on k.zip=q.zip
      and similarity(k.address, q.address_line1) > 0
    where k.zip IN ({in_string} )
),
matches as(
    select
        , property_id,
        , address_similarity,
        , apt_similarity,
        , address_similarity + apt_similarity as total_similarity,
        , k_apt_name,
        , address_hash,
        , address,
        , q_apt_name
        , address_line1
        , zip,
        , row_number() over (partition by property_id order by address_similarity + apt_similarity desc) as match_rank
     from base
    where distinct_pairing = 1 order by 1, 2, 3
)
    insert into {table_name} (
          property_id
        , address_hash
        , address_similarity
        , apt_name_similarity
    ) select
          property_id,
        , address_hash,
        , address_similarity,
        , apt_similarity
     from matches
    where match_rank = 1
      and total_similarity > 0.5;
      """.format(in_string=in_string,
                 table_name=self._table_name)

        self._db_conn.execute(sql)

# end
