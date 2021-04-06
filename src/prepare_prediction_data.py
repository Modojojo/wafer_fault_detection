import pymongo.errors
from src.prediction_validation import Validator


class PreparePredictionData:
    def __init__(self, logger_object, cloud_connect_object, db_connect_object):
        self.cloud = cloud_connect_object
        self.logger = logger_object
        self.db = db_connect_object
        self.prediction_raw_files_dir = 'wafer/data/prediction/'
        self.filenames = None
        self.accepted = []
        self.rejected = []
        self.prediction_files = {}

    def read_filenames(self):
        try:
            self.logger.prediction_pipeline_logs('PREDICTION_DATA_LOAD : Reading Filenames')
            self.filenames = self.cloud.get_file_names(self.prediction_raw_files_dir)
            self.logger.prediction_pipeline_logs('PREDICTION_DATA_LOAD : Reading Filenames --> COMPLETED')
        except pymongo.errors.ServerSelectionTimeoutError as e:
            raise e
        except Exception as e:
            raise Exception('Terminated : Error while reading filenames')

    def read_files(self):
        self.logger.prediction_pipeline_logs('PREDICTION_DATA_LOAD : Reading Files')
        for filename in self.filenames:
            try:
                data = self.cloud.read_data(str(self.prediction_raw_files_dir) + str(filename))
                self.prediction_files[filename] = data
            except Exception as e:
                self.logger.prediction_pipeline_logs('Error while reading File : {}'.format(filename))
        self.logger.prediction_pipeline_logs('PREDICTION_DATA_LOAD : Reading Files --> COMPLETED')

    def create_valid_files(self):
        # Validation
        for filename in self.prediction_files:
            if Validator.validate_file_name(filename) is True:
                dataframe = self.prediction_files[filename]
                if Validator.validate_number_of_columns(dataframe, filename) is True:
                    columns = [str(col).lower() for col in dataframe.columns]
                    columns[0] = 'wafer_id'
                    dataframe.columns = columns
                    self.prediction_files[filename] = dataframe
                    if Validator.validate_name_of_columns(dataframe, filename) is True:
                        if Validator.validate_null_columns(dataframe, filename) is True:
                            try:
                                wafer_id = dataframe['wafer_id']
                                temp_dataframe = dataframe.drop('wafer_id', axis=1)
                                temp_dataframe = temp_dataframe.astype('float')
                                temp_dataframe.insert(0, 'wafer_id', wafer_id)
                                dataframe = temp_dataframe
                                self.prediction_files[filename] = dataframe
                                self.accepted.append(filename)
                            except Exception as e:
                                self.logger.prediction_pipeline_logs(
                                    'PREDICTION_DATA_LOAD : Error while converting data to float :: actual error: '
                                    + str(e))
                                self.rejected.append(filename)
                        else:
                            self.rejected.append(filename)
                    else:
                        self.rejected.append(filename)
                else:
                    self.rejected.append(filename)
            else:
                self.rejected.append(filename)

        self.insert_accepted(self.accepted)
        # self.insert_rejected(self.rejected)

    def insert_accepted(self, accepted_files):
        """
        Inserts Accepted file's into the Database
        :param accepted_files:
        :return:
        """
        self.db.clear_prediction_folder()
        for filename in accepted_files:
            self.db.insert_prediction_data(self.prediction_files[filename])

    def insert_rejected(self, rejected_files):
        """
        Insertes Rejected file's data into Database
        :param rejected_files:
        :return:
        """
        self.db.clear_bad_data_prediction_folder()
        for filename in rejected_files:
            self.db.insert_errored_prediction_data(self.prediction_files[filename])

    def prepare(self):
        self.logger.log_file_validation('---=== PREDICTION FILE VALIDATION PROCESS STARTED ===---')
        try:
            self.read_filenames()
            self.read_files()
            self.create_valid_files()
            return True
        except Exception as e:
            self.logger.prediction_pipeline_logs(
                'PREDICTION_DATA_LOAD : CRITICAL_ERROR : Process Aborted, Data not loaded into the DB ')


