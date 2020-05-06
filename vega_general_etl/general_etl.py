from impala.dbapi import connect
import pandas as pd
import glob
from datetime import date
import os

# Define data path
folder_name = "/tmp/TaxCode/20200415"

# Define file format
file_format = "/*.csv"

SLASH_PATTERN = "/"

# Define database
database = "test_data_taxcode"

try:
    conn = connect(host='node02', port=21050, auth_mechanism='GSSAPI')
    print("=========== Server connected ==========")
    cursor = conn.cursor()
    cursor.execute('CREATE DATABASE IF NOT EXISTS ' + database)
    cursor.execute('USE ' + database)
except:
    print("=========== Connect failed ==========")


def run_cmd(args_list):
    os.system(' '.join(args_list))


files = glob.glob(folder_name+file_format)
for file in files:
    file_path_split = file.split(SLASH_PATTERN)
    hdfs_date_folder = file_path_split[-2]
    hdfs_root_folder = file_path_split[-3]
    file_name = file_path_split[-1]
    file_name_no_format = file_name.split(".")[0]

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
    try:
        print("===== Execute create query =====")
        cursor.execute(query)
        print("Done")
    except:
        print("===== Create table failed! =====")
