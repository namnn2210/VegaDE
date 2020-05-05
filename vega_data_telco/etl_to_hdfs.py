import os
from datetime import date
import glob

path = "data/transformed_data/*.csv"
files = glob.glob(path)

today = str(date.today().strftime("%Y%m%d"))


def run_cmd(args_list):
    os.system(' '.join(args_list))


run_cmd(['hadoop', 'fs', '-mkdir', "/user/hive/warehouse/data_telco_namnn2/data_telco_" + today])
for file in files:
    folder_name = str(file).split("data/transformed_data/")[1].split(".")[0]
    run_cmd(['hadoop', 'fs', '-mkdir', "/user/hive/warehouse/data_telco_namnn2/data_telco_" + today + "/" + str(folder_name)])
    run_cmd(['hadoop', 'fs', '-copyFromLocal', file, "/user/hive/warehouse/data_telco_namnn2/data_telco_" + today + "/" + str(folder_name)])
