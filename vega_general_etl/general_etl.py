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
        print("=========== Server connected ==========")
        return conn
    except Exception as ex:
        print("=========== Connect failed ==========")
        print(ex)
        return None

# Run OS command


def run_cmd(args_list):
    os.system(' '.join(args_list))

# Execute query


def execute_query(conn, query, database):
    try:
        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS ' + str(database))
        cursor.execute('USE ' + str(database))
        cursor.execute(query)
        print("Query executes successfully")
    except Exception as ex:
        print("Query failed" + ex)
    finally:
        cursor.close()
        conn.close()


# Define file format
file_format = "/*.csv"


args = create_args()
files = glob.glob(args['folderpath']+file_format)
conn = impala_connect(
    args['impalahost'], args['impalaport'])


if __name__ == "__main__":
    file_date = os.path.basename(args['folderpath'])
    root_folder = os.path.basename(str(Path(args['folderpath']).parent))
    db_folder = str(args['database'] + ".db")
    process_date = "process_date=" + file_date

    run_cmd(['hdfs', 'dfs', '-mkdir', "/user/hive/warehouse/" +
             db_folder + "/" + root_folder])
    run_cmd(['hdfs', 'dfs', '-mkdir', "/user/hive/warehouse/" +
             db_folder + "/" + root_folder+"/" + process_date])

    for file in files:
        file_name = os.path.basename(file)
        file_name_no_format = os.path.splitext(file_name)[0]
        run_cmd(['hdfs', 'dfs', '-copyFromLocal', '-f', file, "/user/hive/warehouse/" +
                 db_folder + "/" + root_folder+"/" + process_date])

    df = pd.read_csv(files[0], error_bad_lines=False)

    query = "CREATE EXTERNAL TABLE IF NOT EXISTS " + \
        root_folder + "("
    for i in range(len(df.columns)):
        if i == len(df.columns) - 1:
            query += "`" + str(df.columns[i]) + "`" + " string)"
        else:
            query += "`" + str(df.columns[i]) + "`" + " string, "
    query += " PARTITIONED BY (process_date string)"
    query += " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
    query += "LOCATION 'hdfs:///user/hive/warehouse/" + \
        db_folder + "/" + root_folder + "'"
    query += " tblproperties('skip.header.line.count'='1')"

    # Execute query
    print("===== Execute create table query =====")
    execute_query(conn, query, args['database'])

    recover_partition_query = "ALTER TABLE " + root_folder + " RECOVER PARTITIONS"
    print("===== Execute partition query =====")
    execute_query(conn, recover_partition_query, args['database'])
