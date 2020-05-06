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


def impala_connect(impala_host, impala_port, database):
    try:
        conn = connect(host=str(args['impalahost']), port=int(args['impalaport']),
                       auth_mechanism='GSSAPI')
        print("=========== Server connected ==========")
        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS ' + str(database))
        cursor.execute('USE ' + str(database))
        return cursor
    except:
        print("=========== Connect failed ==========")
        return None

# Run OS command


def run_cmd(args_list):
    os.system(' '.join(args_list))

# Execute query


def execute_query(cursor, query):
    try:
        print("===== Execute create query =====")
        cursor.execute(query)
        print("Done")
    except:
        print("===== Create table failed! =====")
    finally:
        cursor.close()


# Define file format
file_format = "/*.csv"


args = create_args()
files = glob.glob(args['folderpath']+file_format)
cursor = impala_connect(
    args['impalahost'], args['impalaport'], args['database'])

for file in files:
    file_name = os.path.basename(file)
    file_name_no_format = os.path.splitext(file_name)[0]
    hdfs_date_folder = os.path.basename(os.path.dirname(file))
    hdfs_root_folder = os.path.basename(
        str(Path(os.path.dirname(file)).parent))

    # Create folder in HDFS
    # Folder format: <table_name> --> <yyyyMMdd> --> <file_name_folder> --> <file_name>")
    # Create <table_name>
    run_cmd(['hadoop', 'fs', '-mkdir', "/user/hive/warehouse/"+hdfs_root_folder])
    # Create <yyyyMMdd>
    run_cmd(['hadoop', 'fs', '-mkdir', "/user/hive/warehouse/" +
             hdfs_root_folder+"/"+hdfs_date_folder])
    # Create <file_name_folder>
    run_cmd(['hadoop', 'fs', '-mkdir', "/user/hive/warehouse/" +
             hdfs_root_folder+"/"+hdfs_date_folder+"/"+file_name_no_format])
    # Copy file from local to HDFS
    run_cmd(['hadoop', 'fs', '-copyFromLocal', file, "/user/hive/warehouse/" +
             hdfs_root_folder+"/"+hdfs_date_folder+"/"+file_name_no_format])

    # Read file
    df = pd.read_csv(file)

    # Create query
    query = "CREATE TABLE IF NOT EXISTS " + file_name_no_format + "("
    for i in range(len(df.columns)):
        if i == len(df.columns) - 1:
            query += str(df.columns[i]) + " string)"
        else:
            query += str(df.columns[i]) + " string, "
    query += " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
    query += "LOCATION 'hdfs:///user/hive/warehouse/" + hdfs_root_folder + \
        "/" + hdfs_date_folder + "/" + file_name_no_format + "'"
    query += " tblproperties('skip.header.line.count'='1')"

    # Execute query
    execute_query(cursor, query)
