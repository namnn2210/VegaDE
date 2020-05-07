from impala.dbapi import connect
import pandas as pd
import glob
import os
import argparse
from pathlib import Path

# Create arguments


def create_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('-fp', '--folderpath', required=True,
                    help='Folder path')
    ap.add_argument('-db', '--database', required=True, help='Database name')
    ap.add_argument('-ih', '--impalahost', required=True,
                    help='Impala hostname')
    ap.add_argument('-ip', '--impalaport', required=True, help='Impala port')
    args = vars(ap.parse_args())
    return args

# Connect to impala


def impala_connect(impala_host, impala_port):
    try:
        conn = connect(host=str(args['impalahost']), port=int(args['impalaport']),
                       auth_mechanism='GSSAPI')
        return conn
    except:
        print("=========== Connect failed ==========")
        return None

# Run OS command


def run_cmd(args_list):
    os.system(' '.join(args_list))

# Execute query


def execute_query(conn, query, database):
    try:
        print("=========== Server connected ==========")
        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS ' + str(database))
        cursor.execute('USE ' + str(database))
        print("===== Execute create query =====")
        cursor.execute(query)
        print("Done")
    except Exception as ex:
        print(ex)
        print("===== Create table failed! =====")
    finally:
        cursor.close()
        conn.close()


# Define file format
file_format = "/*.csv"


args = create_args()
files = glob.glob(args['folderpath']+file_format)
conn = impala_connect(
    args['impalahost'], args['impalaport'])

for file in files:
    file_name = os.path.basename(file)
    file_name_no_format = os.path.splitext(file_name)[0]
    db_folder = str(args['database'] + ".db")
    hdfs_date_folder = os.path.basename(os.path.dirname(file))
    hdfs_root_folder = os.path.basename(
        str(Path(os.path.dirname(file)).parent))
    # Create folder in HDFS
    # Folder format: <table_name> --> <yyyyMMdd> --> <file_name_folder> --> <file_name>")
    # Create <table_name>
    run_cmd(['hadoop', 'fs', '-mkdir',
             "/user/hive/warehouse/"+db_folder + "/" + hdfs_root_folder])
    # Create <yyyyMMdd>
    run_cmd(['hadoop', 'fs', '-mkdir', "/user/hive/warehouse/" + db_folder + "/" +
             hdfs_root_folder+"/" + hdfs_date_folder])
    # Copy file from local to HDFS
    run_cmd(['hadoop', 'fs', '-copyFromLocal', file, "/user/hive/warehouse/" + db_folder + "/" +
             hdfs_root_folder+"/" + hdfs_date_folder])

    # Read file
    df = pd.read_csv(file, error_bad_lines=False)

    # Create query
    query = "CREATE EXTERNAL TABLE IF NOT EXISTS " + file_name_no_format + "("
    for i in range(len(df.columns)):
        if i == len(df.columns) - 1:
            query += str(df.columns[i]) + " string)"
        else:
            query += str(df.columns[i]) + " string, "
    query += " PARTITIONED BY (file_date string)"
    query += " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
    query += "LOCATION 'hdfs:///user/hive/warehouse/" + \
        db_folder + "/" + hdfs_root_folder + "'"
    query += " tblproperties('skip.header.line.count'='1')"

    # Execute query
    execute_query(conn, query, args['database'])

    recover_partition_query = "ALTER TABLE " + \
        file_name_no_format + \
        " ADD PARTITION(file_date=" + "'" + hdfs_date_folder + "'" + ")" + " LOCATION 'hdfs:///user/hive/warehouse/" + \
        db_folder + "/" + hdfs_root_folder + "/" + hdfs_date_folder + "'"

    print("===== Execute partition query =====")
    execute_query(conn, recover_partition_query, args['database'])
