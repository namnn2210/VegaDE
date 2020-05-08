import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import *
import re
import glob
import os
import shutil

# Read multiple files
# folder_path = glob("data/*.xls")
# list_df = [pd.read_excel(file) for file in folder_path]
# print(list_df)

sample_df_columns = ['ten_kh', 'dia_chi', 'sdt', 'thanh_pho']

empty_df = pd.DataFrame(columns=sample_df_columns)

spark = SparkSession.builder.master("local[*]").appName("Excel").getOrCreate()
sparkContext = spark.sparkContext

bool_exists_header = True

schema = StructType([
    StructField("ten_kh", StringType()),
    StructField("dia_chi", StringType()),
    StructField("sdt", LongType()),
    StructField("thanh_pho", StringType())
])


# Get column quantity
def get_col_quantity(spark_dataframe):
    return len(spark_dataframe.columns)


# Generate column name
def generate_col_name(pandas_df):
    number_columns = len(pandas_df.columns)
    match_column = sample_df_columns[:number_columns]
    return match_column


# Check if first column is index column
def check_first_col(spark_dataframe):
    if len(str(spark_dataframe.columns[0]).strip()) == 0:
        return spark_dataframe.drop(spark_dataframe.columns[0])
    return spark_dataframe


# Check if header exists
def exist_header(pandas_df_columns):
    for index in pandas_df_columns:
        try:
            int(index)
            return False
        except ValueError:
            continue


# Add headers if not exists
def add_header(pandas_df):
    bool_exists_header = exist_header(pandas_df.columns)
    if bool_exists_header == False:
        match_column = generate_col_name(pandas_df)
        pandas_df = pd.DataFrame(np.row_stack([pandas_df.columns, pandas_df.values]),
                                 columns=match_column)
        return pandas_df
    else:
        return pandas_df


# Remove vietnamese accent
def no_accent_vietnamese(s):
    s = re.sub(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
    s = re.sub(r'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
    s = re.sub(r'[èéẹẻẽêềếệểễ]', 'e', s)
    s = re.sub(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
    s = re.sub(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
    s = re.sub(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
    s = re.sub(r'[ìíịỉĩ]', 'i', s)
    s = re.sub(r'[ÌÍỊỈĨ]', 'I', s)
    s = re.sub(r'[ùúụủũưừứựửữ]', 'u', s)
    s = re.sub(r'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
    s = re.sub(r'[ỳýỵỷỹ]', 'y', s)
    s = re.sub(r'[ỲÝỴỶỸ]', 'Y', s)
    s = re.sub(r'[Đ]', 'D', s)
    s = re.sub(r'[đ]', 'd', s)
    return s


# Reorder value to field
def reorder_value_to_field(spark_dataframe):
    corrected_col = {}
    list_col = spark_dataframe.columns
    count_matches_address = 0
    count_matches_name = 0
    count_matches_city = 0
    count_unknown = 0
    for i in range(len(list_col)):
        row_val = spark_dataframe.select(list_col[i]).collect()
        col_val = [ele[list_col[i]] for ele in row_val]
        for val in col_val:
            try:
                int(str(val))
                corrected_col["sdt"] = col_val
                break
            except ValueError:
                no_whitespace_val = str(val).replace(" ", "").lower()
                if (no_whitespace_val.count("/") > 2) or (
                        no_whitespace_val.count(",") > 2) or (no_whitespace_val.count(".") > 2
                                                              ):
                    corrected_col["dia_chi"] = col_val
                    break
                elif (
                        "-" in no_whitespace_val and "/" in no_whitespace_val and "," in no_whitespace_val and "." in no_whitespace_val) or (
                        "-" in no_whitespace_val and "/" in no_whitespace_val and "," in no_whitespace_val) or (
                        "-" in no_whitespace_val and "/" in no_whitespace_val and "." in no_whitespace_val) or (
                        "/" in no_whitespace_val and "," in no_whitespace_val and "." in no_whitespace_val) or (
                        "-" in no_whitespace_val and "," in no_whitespace_val and "." in no_whitespace_val) or (
                        "-" in no_whitespace_val and "/" in no_whitespace_val) or (
                        "-" in no_whitespace_val and "," in no_whitespace_val) or (
                        "-" in no_whitespace_val and "." in no_whitespace_val) or (
                        "/" in no_whitespace_val and "," in no_whitespace_val) or (
                        "/" in no_whitespace_val and "." in no_whitespace_val) or (
                        "," in no_whitespace_val and "." in no_whitespace_val):
                    count_matches_address += 1
                    if count_matches_address >= 4:
                        if "dia_chi" not in corrected_col.keys():
                            corrected_col["dia_chi"] = col_val
                            count_matches_address = 0
                            break
                        else:
                            corrected_col["tmp_dia_chi"] = col_val
                            break
                else:
                    remove_accent_str = no_accent_vietnamese(no_whitespace_val)
                    if remove_accent_str in list_cities:
                        count_matches_city += 1
                        if count_matches_city >= 4:
                            if "thanh_pho" not in corrected_col.keys():
                                corrected_col["thanh_pho"] = col_val
                                count_matches_city = 0
                                break
                            else:
                                corrected_col["tmp_thanh_pho"] = col_val
                                break
                    elif ("-" not in no_whitespace_val) or ("." not in no_whitespace_val) or (
                            "/" not in no_whitespace_val):
                        count_matches_name += 1
                        if count_matches_name >= 4:
                            if "ten_kh" not in corrected_col.keys():
                                corrected_col["ten_kh"] = col_val
                                count_matches_name = 0
                                break
                            else:
                                count_unknown += 1
                                if count_unknown >= 4:
                                    corrected_col["tmp_ten_kh"] = col_val
                                    break
    spark_dataframe = spark.createDataFrame(pd.DataFrame(corrected_col))
    return spark_dataframe


# Reorder columns to schema
def reorder_columns(spark_dataframe):
    return spark_dataframe.toPandas().reindex(columns=sample_df_columns)


# Format column name:
def format_column_name(spark_dataframe):
    match_column = generate_col_name(spark_dataframe.toPandas())
    return spark_dataframe.toDF(*match_column)


# Create excel columns
def create_new_col(spark_dataframe):
    df_columns = spark_dataframe.columns
    if len(df_columns) < 4:
        no_new_col = 4 - len(df_columns)
        for i in range(no_new_col):
            spark_dataframe = spark_dataframe.withColumn(
                "tmp_col_" + str(4 + i), lit(0))
        return spark_dataframe
    else:
        return spark_dataframe


# Format phone number
def format_phone_no(spark_dataframe):
    if "sdt" in spark_dataframe.columns:
        get_phone_col = spark_dataframe.select("sdt").collect()
        for i in range(len(get_phone_col)):
            phone_head = get_phone_col[i].sdt[:2]
            phone_tail = get_phone_col[i].sdt[-2:]
            if phone_head == '84':
                if phone_tail == '.0':
                    get_phone_col[i] = "0" + str(get_phone_col[i].sdt[2:-2])
                else:
                    get_phone_col[i] = "0" + str(get_phone_col[i].sdt[2:])
            else:
                get_phone_col[i] = "0" + str(get_phone_col[i].sdt)
        new_phone_df = spark.createDataFrame(zip(get_phone_col), ["sdt_new"])
        new_pandas_df = spark_dataframe.toPandas().join(new_phone_df.toPandas())
        return spark.createDataFrame(new_pandas_df.astype(str)).drop("sdt").withColumnRenamed("sdt_new", "sdt")
    else:
        return spark_dataframe


# Write to CSV
def write_to_csv(spark_dataframe):
    spark_dataframe.toPandas().to_csv('data/transformed_data/' +
                                      file_table_name + '.csv', index=False)


cities_path = "cities.xlsx"
cities_data = pd.read_excel(cities_path)
list_cities = []
for city in list(cities_data['City']):
    new_city = no_accent_vietnamese(str(city).replace(" ", "")).lower()
    list_cities.append(new_city)

if __name__ == '__main__':
    path = "data/*.xls*"
    files = glob.glob(path)
    try:
        os.mkdir("data/transformed_data")
    except FileExistsError:
        shutil.rmtree("data/transformed_data")
        os.mkdir("data/transformed_data")

    for file in files:
        get_name = no_accent_vietnamese(file.split(
            "data/")[1].split(".")[0].replace(" ", "").lower())
        file_table_name = 'data_telco_' + get_name
        data = pd.read_excel(file)
        data_df_spark = spark.createDataFrame(data.astype(str))
        checked_first_col_df = check_first_col(data_df_spark)
        bool_exists_header = exist_header(data_df_spark.toPandas())
        if bool_exists_header == None:
            formatted_col_name_df = format_column_name(checked_first_col_df)
            if get_col_quantity(formatted_col_name_df) == len(schema.fields):
                formatted_phone_col_df = format_phone_no(formatted_col_name_df)
                final_dataframe = spark.createDataFrame(
                    reorder_columns(formatted_phone_col_df))
                write_to_csv(final_dataframe)
            else:
                reordered_df = reorder_value_to_field(formatted_col_name_df)
                final_dataframe = format_phone_no(reordered_df)
                write_to_csv(final_dataframe)
        else:
            added_header_df = add_header(checked_first_col_df.toPandas())
            reordered_df = reorder_value_to_field(
                spark.createDataFrame(added_header_df.astype(str)))
            final_dataframe = format_phone_no(reordered_df)
            write_to_csv(final_dataframe)
