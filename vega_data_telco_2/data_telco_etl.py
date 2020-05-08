import pandas as pd
import os
import shutil
import re
import argparse
import glob


sample_columns = ['fullname', 'address', 'phone', 'city']


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


# Check if file has required column quantity
def enough_columns(dataframe):
    return len(dataframe.columns) == len(sample_columns)


# Check if file has header
def exist_header(dataframe):
    for index in dataframe:
        try:
            int(index)
            return False
        except ValueError:
            return True


# Move file to error folder
def to_error_folder(file, folderpath):
    print(file)
    run_cmd(['cp', file, folderpath + '/' + 'errors_files'])


# Format column name:
def format_column_name(dataframe):
    dataframe.columns = sample_columns
    return dataframe


# Write to CSV
def write_csv(dataframe, file, folderpath):
    new_file_name = get_file_name_without_extension(get_file_name(file))
    print(file)
    dataframe.to_csv(folderpath + '/' + "transformed_data/" +
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
    return string.replace(" ", "").lower()


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
                run_cmd(['mv', args['folderpath']+'/'+file_name_with_space, args['folderpath'] + '/' + "data_telco_" +
                         new_file_name + get_file_name_extension(get_file_name(file))])
            else:
                run_cmd(['mv', file, args['folderpath'] + '/' + "data_telco_" +
                         new_file_name + get_file_name_extension(get_file_name(file))])


# Get files from folder:
def get_files(folderpath, file_format):
    return glob.glob(folderpath+file_format)


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
    dataframe['phone'] = get_phone_col


if __name__ == "__main__":
    args = create_args()
    file_format = "/*.xls*"
    raw_files = get_files(args['folderpath'], file_format)
    create_error_folder(args['folderpath'])
    create_transformed_folder(args['folderpath'])
    rename_files(raw_files, args['folderpath'])
    renamed_files = get_files(args['folderpath'], file_format)
    list_cities = get_list_cities()
    for file in renamed_files:
        df = pd.read_excel(file, error_bad_lines=False)
        check_first_col(df)
        if enough_columns(df) and exist_header(df):
            format_column_name(df)
            format_phone_no(df)
            print("Correct files")
            write_csv(df, file, str(args['folderpath']))
        # elif exist_header(df):

        else:
            print("Error files")
            to_error_folder(file, str(args['folderpath']))
