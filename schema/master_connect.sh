cat secrets.txt
USER=aptmerge_master
HOST=aptmerge2.cyhyspsglw2i.us-east-1.rds.amazonaws.com
DBNAME=aptmerge
/Applications/Postgres.app/Contents/Versions/10/bin/psql --port=5432 --host=$HOST -U $USER --password --dbname=$DBNAME 



