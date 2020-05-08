import pandas as pd
from data_telco_etl import modify_string, no_accent_vietnamese, get_list_cities
path = "data/data_telco_caobang.xls"

dataframe = pd.read_excel(path)
list_col = dataframe.columns
count_city = 0
list_cities = get_list_cities()
for i in range(len(list_col)):
    col_val = dataframe[list_col[i]].astype(str)
    for val in col_val:
        try:
            int(val)
            dataframe.rename(columns={list_col[i]: 'phone'}, inplace=True)
            break
        except ValueError:
            no_whitespace_val = modify_string(val)
            remove_accent_str = no_accent_vietnamese(no_whitespace_val)
            if remove_accent_str in list_cities:
                count_city += 1
                if count_city >= 4:
                    dataframe.rename(
                        columns={list_col[i]: 'city'}, inplace=True)
                    count_city = 0
                    break

print(dataframe.head(5))
