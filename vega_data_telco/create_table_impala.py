from impala.dbapi import connect
import glob
from datetime import date

conn = connect(host='node02', port=21050, auth_mechanism='GSSAPI')
cursor = conn.cursor()

database = "data_telco"

cursor.execute("use "+database)
path = "data/transformed_data/*.csv"
files = glob.glob(path)

today = str(date.today().strftime("%Y%m%d"))

for file in files:
    folder_name = str(file).split("data/transformed_data/")[1].split(".")[0]
    query = 'CREATE EXTERNAL TABLE IF NOT EXISTS ' + folder_name + '(`ten_kh` string, `dia_chi` string, `sdt` bigint, `thanh_pho` string)' + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " + "LOCATION 'hdfs:///user/hive/warehouse/data_telco_namnn2/data_telco_" + today + "/" + folder_name + "'"
    cursor.execute(query)