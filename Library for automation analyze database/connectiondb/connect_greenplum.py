

#connection greenplum


import psycopg2

HOST="HOST", 
PORT="PORT", 
DBNAME="DBNAME", 
USER="gpadmin", 
PASSWORD="PASSWORT"


psycopg2.connect(host=HOST, port=PORT, dbname=DBNAME, user=USER, password=PASSWORD)