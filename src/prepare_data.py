from src.validator import Validator
from src.db_connect import DbConnector
import pymongo.errors


class PrepareData:
    def __init__(self, training_raw_files_directory, logger_object, cloud_connect_object):
        self.cloud = cloud_connect_object
        self.logger = logger_object
        self.filenames = None
        self.raw_files_dir = training_raw_files_directory
        self.training_files = {}
        self.accepted = []
        self.rejected = []
        self.db = DbConnector()

    def move_to_rejected_list(self, rejected_files):
        """
        Inputs the names of the Raw Files that are rejected, Moves them from accepted list to Rejected list
        :param rejected_files: List of filenames of Rejected Raw data files
        :return: None
        """
        while True:
            try:
                filename = rejected_files.pop()
            except IndexError:
                break
            self.accepted.remove(filename)
            self.rejected.append(filename)
        return

    def read_filenames(self):
        """
        Reads the file names from Server's Folder Provided while initializing Class.
        :return:
        """
        try:
            self.logger.pipeline_logs('Reading Filenames')
            self.filenames = self.cloud.get_file_names(self.raw_files_dir)
            self.logger.pipeline_logs('Reading Filenames --> COMPLETED')
        except pymongo.errors.ServerSelectionTimeoutError as e:
            raise e
        except Exception as e:
            raise Exception('Terminated : Error while reading filenames')

    def read_files(self):
        self.logger.pipeline_logs('Reading Files')
        for filename in self.filenames:
            try:
                data = self.cloud.read_data(str(self.raw_files_dir) + str(filename))
                self.training_files[filename] = data
            except Exception as e:
                self.logger.pipeline_logs('Error while reading File : {}'.format(filename))
        self.logger.pipeline_logs('Reading Files --> COMPLETED')

    #
    #
    # Note : This code needs to be optmized and put inside one for loop Currently multiple for loops are used
    #
    #
    def create_valid_files(self):
        """
        Validates the Files and moves it to Good data (Training) and Bad Data collections in the DataBase
        :return:
        """
        self.logger.pipeline_logs('Raw File Validation : STARTED : Goto raw_validation_logs collection for detailed logs')
        # Filename Validation
        for filename in self.training_files:
            if Validator.validate_file_name(filename) is True:
                self.accepted.append(filename)
            else:
                self.rejected.append(filename)

        rejected = []
        # validate number of columns
        for filename in self.accepted:
            data = self.training_files[filename]
            if Validator.validate_number_of_columns(data, filename) is False:
                rejected.append(filename)

        # move rejected files to rejected list
        self.move_to_rejected_list(rejected)
        rejected = []

        # Validate column names
        for filename in self.accepted:
            data = self.training_files[filename]
            columns = [str(col).lower() for col in data.columns]
            columns[0] = 'wafer_id'
            columns[-1] = 'class'
            data.columns = columns
            self.training_files[filename] = data
            if Validator.validate_name_of_columns(data, filename) is False:
                rejected.append(filename)

        # move rejected files to rejected list
        self.move_to_rejected_list(rejected)
        rejected = []

        # validate if any column is completely null in the data
        for filename in self.accepted:
            data = self.training_files[filename]
            if Validator.validate_null_columns(data, filename) is False:
                rejected.append(filename)

        # move rejected files to rejected list
        self.move_to_rejected_list(rejected)

        for filename in self.accepted:
            dataframe = self.training_files[filename]
            wafer_id = dataframe['wafer_id']
            classes = dataframe['class']
            temp_dataframe = dataframe.drop(['wafer_id', 'class'], axis=1)
            try:
                temp_dataframe = temp_dataframe.astype('float')
                dataframe = temp_dataframe
                dataframe['wafer_id'] = wafer_id
                dataframe['class'] = classes
                self.training_files[filename] = dataframe
            except Exception as e:
                self.logger.log_file_validation('Cannot convert data types to float | file : {}'.format(filename))

        self.logger.pipeline_logs('Raw File Validation : COMPLETED')

        self.logger.pipeline_logs('Raw File Validation : COMPLETED')
        self.logger.pipeline_logs('Total number of files Accepted : {}'.format(len(self.accepted)))
        self.logger.pipeline_logs('Total number of files Rejected : {}'.format(len(self.rejected)))

        self.logger.pipeline_logs('Data insertion into DB : STARTED')
        self.insert_accepted(self.accepted)
        self.logger.pipeline_logs('Data insertion into DB [ GOOD DATA (Training Files) : COMPLETED] ')
        #self.insert_rejected(self.rejected)
        self.logger.pipeline_logs('Data insertion into DB [ BAD DATA (Errored files): COMPLETED]')
        self.logger.pipeline_logs('Data insertion into DB : COMPLETED')
        return

    def insert_accepted(self, accepted_files):
        """
        Inserts Accepted file's into the Database
        :param accepted_files:
        :return:
        """
        self.db.clear_training_folder()
        for filename in accepted_files:
            self.db.insert_training_data(self.training_files[filename])

    def insert_rejected(self, rejected_files):
        """
        Insertes Rejected file's data into Database
        :param rejected_files:
        :return:
        """
        self.db.clear_bad_data_folder()
        for filename in rejected_files:
            self.db.insert_errored_data(self.training_files[filename])
        pass

    def prepare(self):
        """
        Main Function that Performs the Complete data Preparation and Loading into the DB operations
        :return: True if all the the Operations have been Performed Successfully.
        """
        self.logger.log_file_validation('---=== PROCESS STARTED ===---')
        self.read_filenames()
        self.read_files()
        self.create_valid_files()
        return True












