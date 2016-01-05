#/usr/bin/python2.7
#
import sys
import getopt
import json

import psycopg2
from datetime import datetime

_debug = False
params = None


def process_arguments(database, username=None, passw=None, h="localhost", p="5432"):
    # Try to connect to the database
    try:
        conn = psycopg2.connect(dbname=database,
                                user=username, 
                                password=passw,
                                host=h,
                                port=p)
    except:
        print "I am unable to connect to the database."
        sys.exit(2)

    metadata_cur = conn.cursor()
    schema_cur = conn.cursor()

    try:
        metadata_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        conn.commit()
    except:
        print "I can't select metadata! Is the database empty?"
        sys.exit(2)

    metadata_rows = metadata_cur.fetchall()
    table_list = []
    table_count = 0

    for each_entry in metadata_rows:
        table_dict = {}

        table_name = each_entry[0]
        table_dict["name"] = table_name

        query = "SELECT COUNT(*) FROM %s;" % (table_name, )

        # Get row count from table
        try:
            schema_cur.execute(query)
            conn.commit()
        except:
            print "I can't select rows!"

        schema_rows = schema_cur.fetchall()
        row_count = 0

        for each_count in schema_rows:
            row_count = each_count[0]

        table_dict["count"] = row_count

        table_list.append(table_dict)
        table_count += 1

    print "Number of tables: %s" % (table_count, )

    for each_table_dict in table_list:
        print "%s: %s" % (each_table_dict["name"], each_table_dict["count"])


def main(argv):
    # Global debug flag allows you to use one debug value throughout your code
    global _debug
    global params

    no_args = True
    dbname = None
    user = None
    password = None
    host = "localhost"
    port = "5432"

    # Getopt allows you to specify flag, full argument type, and input :args
    try:
        opts, args = getopt.getopt(argv, "hD:u:p:H:P:d",
                                   ["help", "database", "username", "password", "hostname", "port"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    # Each if statement handles an option opt and an :arg as per above
    # If you wanted something to happen with just an option and no
    # arguments, you'd handle it here
    for opt, arg in opts:
        if (opt in ("-h", "--help")):
            usage()
            sys.exit(2)
        elif (opt in ("-D", "--database")):
            dbname = arg
            no_args = False
        elif (opt in ("-u", "--username")):
            user = arg
            no_args = False
        elif (opt in ("-p", "--password")):
            password = arg
            no_args = False
        elif (opt in ("-H", "--host")):
            host = arg
            no_args = False
        elif (opt in ("-P", "--port")):
            port = arg
            no_args = False
        elif (opt == "-d"):
            _debug = True

    if (no_args):
        # Sometimes you need arguments; sometimes you don't!
        # Default behavior for no arguments would go here
        # In this case we require args so we print out the usage
        usage()
        sys.exit(2)
    else:
        # This is where all the argument handling happens
        if (dbname):
            process_arguments(dbname, user, password, host, port)
        else:
            usage()
            sys.exit(2)


def usage():
    print "usage: python pg_metadata.py -D database [-u username] [-p password] [-H hostname] [-P port]..."
    print "Options and arguments:"
    print "-D database : DB name (also --database)"
    print "-u username : DB user (also --username)"
    print "-p password : DB password (also --password)"
    print "-H hostname : DB password (also --hostname)"
    print "-P port : DB password (also --port)"
    print "-d : debug output"
    print "-h : print this help message and exit (also --help)"


if __name__ == "__main__":
    main(sys.argv[1:])
