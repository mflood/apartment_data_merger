# adds required environment variables
# to use, run:
#
#   source envs.sh
#   set | grep GCP_
# ENV Variables that are set in the Docker container
export GCP_BUCKET=data_engineer_assessment
export GCP_FILE_SNOWFLAKE_TABLE=snowflake_table.txt
export GCP_FILE_SQLSERVER_TABLE=sqlserver_table.txt
