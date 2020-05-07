import pandas as pd
import os
import shutil
path = "data/angiang.xls"

columns = ['fullname', 'address', 'phone', 'city']

# Run OS command


def run_cmd(args_list):
    os.system(' '.join(args_list))


# Get column quantity


def get_col_quantity(dataframe):
    return len(dataframe.columns)

# Check if file has required column quantity


def enough_columns(dataframe):
    return len(dataframe.columns) == len(columns)

# Check if file has header


def exist_header(dataframe):
    for index in dataframe:
        try:
            int(index)
            return False
        except ValueError:
            return True

# Copy file to error folder


def to_error_folder(path):
    file_name = os.path.basename(path)
    try:
        os.mkdir("data/errors_file")
    except FileExistsError:
        shutil.rmtree("data/errors_file")
        os.mkdir("data/errors_file")
    run_cmd(['mv', file_name, 'errors_file'])

# Format column name:


def format_column_name(dataframe):
    return dataframe.toDF(*columns)


if __name__ == "__main__":
    df = pd.read_excel(path)
    if enough_columns(df) and exist_header(df):

    else:
        to_error_folder(path)
