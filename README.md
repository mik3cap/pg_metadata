# pg_metadata
Python script for analyzing Postgres DB metadata. Requires psycopg2 module, which you can install with:

`pip install psycopg2`

**usage:** `python pg_metadata.py -D database [-u username] [-p password] [-H hostname] [-P port]...`

**Options and arguments:**
-  -D database : DB name (also --database)
-  -u username : DB user (also --username)
-  -p password : DB password (also --password)
-  -H hostname : DB hostname (also --hostname)
-  -P port : DB port (also --port)
-  -d : debug output
-  -h : print this help message and exit (also --help)
  
