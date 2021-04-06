from src.custom_logger import Logger
import re
import json

with open('config/training_config.json') as f:
    config = json.loads(f.read())

NUMBER_OF_COLUMNS = config['numberOfColumns']
COLUMN_NAMES = [name for name in config['columns']]
COLUMN_DATA_TYPES = config['columns']
DATA_TYPE_DICT = {'object': object, 'float': float, "int": int}

logger = Logger()
log_type = 'file_validation'


class Validator:

    @staticmethod
    def validate_file_name(filename):
        """
        Perform Filename and filetype Validation
        :param filename: Exact Name of the file with extension
        :return: True if Filename and type are valid, else raise Exception
        """
        pattern = r'^wafer_[0-9]{8}_[0-9]{6}\.csv$'
        try:
            if not re.match(pattern, str(filename).lower()):
                raise Exception('TRAINING_VALIDATION : Invalid Filename/Filetype' + ':' + str(filename))
            else:
                return True
        except Exception as e:
            logger.log_file_validation(str(e))
            return False

    @staticmethod
    def validate_number_of_columns(df, filename):
        """
        Validate the number of columns in the dataset
        :param df: Pandas Dataframe
        :return: True if valid, else Raise Exception
        """
        try:
            if not len(df.columns) == NUMBER_OF_COLUMNS:
                raise Exception('TRAINING_VALIDATION : Invalid Number of Columns in file : ' + str(filename))
            else:
                return True
        except Exception as e:
            logger.log_file_validation(str(e))
            return False

    @staticmethod
    def validate_name_of_columns(df, filename):
        columns = df.columns
        try:
            if columns.tolist() == COLUMN_NAMES:
                return True
            else:
                raise Exception('TRAINING_VALIDATION : Invalid Columns Names in file : ' + str(filename))
        except Exception as e:
            logger.log_file_validation(str(e))
            return False

    @staticmethod
    def validate_column_data_type(df, filename):
        columns = df.columns
        try:
            col_data_types = df.dtypes
            for index in col_data_types.index:
                if col_data_types[index] != DATA_TYPE_DICT[COLUMN_DATA_TYPES[index]]:
                    raise Exception('TRAINING_VALIDATION : Invalid columns data type in file : {}'.format(filename))
            return True
        except Exception as e:
            logger.log_file_validation(str(e))
            return False

    @staticmethod
    def validate_null_columns(df, filename):
        try:
            if len(df.isnull().sum()[df.isnull().sum() == len(df)]) == 0:
                return True
            else:
                raise Exception('TRAINING_VALIDATION : All Column Values are null in file : {}'.format(filename))
        except Exception as e:
            logger.log_file_validation(str(e))
            return False



