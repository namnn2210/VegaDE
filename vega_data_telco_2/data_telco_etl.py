# -*- coding: utf-8 -*-
import pandas as pd
import os
import shutil
import re
import argparse
import glob


# Create arguments
def create_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('-fp', '--folderpath', required=True,
                    help='Folder path')
    args = vars(ap.parse_args())
    return args


# Run OS command
def run_cmd(args_list):
    os.system(' '.join(args_list))


# Get column quantity
def get_col_quantity(dataframe):
    return len(dataframe.columns)


# Check if file has header
def exist_header(dataframe):
    count = 0
    if count == len(dataframe.columns):
        return True
    for index in list(dataframe.columns):
        try:
            int(index)
            return False
        except ValueError:
            count += 1
            continue


# Move file to error folder
def to_error_folder(file, folderpath):
    print(file)
    run_cmd(['cp', file, folderpath + '/' + 'errors_files'])


# Write to CSV final
def write_csv_final(dataframe, file, folderpath):
    new_file_name = get_file_name_without_extension(get_file_name(file))
    print(file)
    dataframe.to_csv(folderpath + '/' + "transformed_data/" +
                     new_file_name + ".csv", index=False)


# Write to CSV before apply schema
def write_csv_schema(dataframe, file, folderpath):
    new_file_name = get_file_name_without_extension(get_file_name(file))
    print(file)
    dataframe.to_csv(folderpath + '/' + "pending_schema_data/" +
                     new_file_name + ".csv", index=False)


# Get file name
def get_file_name(path):
    return os.path.basename(path)


# Get file name without extension
def get_file_name_without_extension(file_name):
    return os.path.splitext(file_name)[0]


# Get file extension
def get_file_name_extension(file_name):
    return os.path.splitext(file_name)[1]


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


# Remove space and lower case
def modify_string(string):
    return string.replace(" ", "").replace("_", "").lower()


# Create error folder
def create_error_folder(folderpath):
    try:
        os.mkdir(folderpath + '/' + "errors_files")
    except FileExistsError:
        shutil.rmtree(folderpath + '/' + "errors_files")
        os.mkdir(folderpath + '/' + "errors_files")


# Create transformed folder
def create_transformed_folder(folderpath):
    try:
        os.mkdir(folderpath + '/' + "transformed_data")
    except FileExistsError:
        shutil.rmtree(folderpath + '/' + "transformed_data")
        os.mkdir(folderpath + '/' + "transformed_data")


# Create renamed folder
def create_renamed_folder(folderpath):
    try:
        os.mkdir(folderpath + '/' + "renamed_folder")
    except FileExistsError:
        shutil.rmtree(folderpath + '/' + "renamed_folder")
        os.mkdir(folderpath + '/' + "renamed_folder")


# Create write schema folder
def create_pending_folder(folderpath):
    try:
        os.mkdir(folderpath + '/' + "pending_schema_data")
    except FileExistsError:
        shutil.rmtree(folderpath + '/' + "pending_schema_data")
        os.mkdir(folderpath + '/' + "pending_schema_data")


# Rename all files in folder path
def rename_files(files, folderpath):
    file_name_with_space = ''
    for file in files:
        if not get_file_name(file).startswith('data_telco_'):
            new_file_name = no_accent_vietnamese(
                get_file_name_without_extension(get_file_name(modify_string(file))))
            file_extension = get_file_name_extension(get_file_name(file))
            if ' ' in file:
                file_name_split = get_file_name(file).split(' ')
                separator = '\ '
                file_name_with_space = separator.join(file_name_split)
                run_cmd(['cp', args['folderpath'] + '/' + file_name_with_space,
                         args['folderpath'] + '/' + 'renamed_folder' + '/' + "data_telco_" +
                         new_file_name + get_file_name_extension(get_file_name(file))])
            else:
                run_cmd(['cp', file, args['folderpath'] + '/' + 'renamed_folder' + '/' + "data_telco_" +
                         new_file_name + get_file_name_extension(get_file_name(file))])
        else:
            new_file_name = no_accent_vietnamese(get_file_name(modify_string(file)))
            run_cmd(['cp', file, args['folderpath'] + '/' + 'renamed_folder' + '/' +
                     new_file_name])


# Get files from folder:
def get_files(folderpath, file_format):
    return glob.glob(folderpath + file_format)


# Check if first column is index column
def check_first_col(dataframe):
    if len(str(dataframe.columns[0]).strip()) == 0:
        return dataframe.__delitem__(df.columns[0])
    return dataframe


# Get list cities:
def get_list_cities():
    cities_path = "cities.xlsx"
    cities_data = pd.read_excel(cities_path)
    list_cities = []
    for city in list(cities_data['City']):
        new_city = no_accent_vietnamese(str(city).replace(" ", "")).lower()
        list_cities.append(new_city)
    return list_cities


# Format phone number
def format_phone_no(dataframe):
    get_phone_col = dataframe['phone'].astype(str)
    for i in range(len(get_phone_col)):
        phone_head = str(get_phone_col[i])[:2]
        if phone_head == '84':
            get_phone_col[i] = '0' + str(get_phone_col[i])[2:]
        else:
            get_phone_col[i] = '0' + str(get_phone_col[i])[2:]
    dataframe['phone'] = get_phone_col


# Detect column:
def detect_column(dataframe):
    list_cities = get_list_cities()
    count_city = 0
    list_col = dataframe.columns
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


# Create a dictionary to detect column name
column_detect_dict = {
    'fullname': ['fullname', 'name', 'ten', 'tenkh', 'tentb', 'tentt'],
    'address': ['address', 'diachi', 'diachitt', 'diachikh', 'diachitb', 'khachhang'],
    'phone': ['phone', 'sdt', 'somay', 'matb', 'mobile', 'smdaidien', 'didong', 'sodaidien'],
    'city': ['city', 'thanhpho', 'tinh', 'matinh']
}


# Detect column name by dictionary
def detect_column_by_dict(dataframe):
    columns = dataframe.columns
    for column in columns:
        for key, values in column_detect_dict.items():
            for value in values:
                if value == no_accent_vietnamese(modify_string(column)):
                    dataframe.rename(columns={column: key}, inplace=True)


# Get col difference
def difference_extra_credit(l1, l2):
    list = l1 + l2
    return [value for value in list if (value in l1) ^ (value in l2)]


# Create schema
def dataframe_process(dataframe, list_col_tmp):
    detect_column_by_dict(dataframe)
    new_cols = difference_extra_credit(list_col_tmp, list(dataframe.columns))
    for col in new_cols:
        if col not in list_col_tmp:
            list_col_tmp.append(col)


if __name__ == "__main__":
    args = create_args()
    file_format = "/*.xls*"
    raw_files = get_files(args['folderpath'], file_format)
    create_error_folder(args['folderpath'])
    create_transformed_folder(args['folderpath'])
    create_renamed_folder(args['folderpath'])
    create_pending_folder(args['folderpath'])
    rename_files(raw_files, args['folderpath'])
    renamed_files = get_files(args['folderpath'] + '/' + 'renamed_folder', file_format)
    list_col_tmp = []
    # Classifying files
    for file in renamed_files:
        df = pd.read_excel(file, error_bad_lines=False)
        check_first_col(df)
        bool_exists_header = exist_header(df)
        if bool_exists_header == None:
            dataframe_process(df, list_col_tmp)
            print("Correct files")
            write_csv_schema(df, file, str(args['folderpath']))
        else:
            print("Error files")
            to_error_folder(file, str(args['folderpath']))
    files_for_schema = get_files(args['folderpath'] + '/' + 'pending_schema_data', '/*.csv')
    # Apply final schema
    print("========== Applying schema ==========")
    for file in files_for_schema:
        df = pd.read_csv(file, error_bad_lines=False)
        for col in list_col_tmp:
            if col not in list(df.columns):
                df[col] = ''
        write_csv_final(df, file, str(args['folderpath']))
    # Remove pending folder
    shutil.rmtree(args['folderpath'] + '/' + "pending_schema_data")
    # Remove renamed folder
    shutil.rmtree(args['folderpath'] + '/' + "renamed_folder")
    print("======================================")
    print("Total files: " + str(len(get_files(args['folderpath'], file_format))))
    print("Transformed files: " + str(len(get_files(args['folderpath'] + '/' + 'transformed_data', '/*.csv'))))
    print("Error files: " + str(len(get_files(args['folderpath'] + '/' + 'errors_files', file_format))))