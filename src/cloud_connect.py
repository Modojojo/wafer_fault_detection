import pandas as pd
from boto3.session import Session
import os
import pickle
import json

access_key = os.environ.get("CLOUD_ACCESS_KEY")
secret_access_key = os.environ.get("CLOUD_SECRET_ACCESS_KEY")
bucket_name = os.environ.get("CLOUD_BUCKET_NAME")


class Cloud:
    def __init__(self):
        self.access_key = access_key
        self.secret_access_key = secret_access_key
        self.session = None
        self.s3_resource = None
        self.bucket_name = bucket_name
        self.bucket = None
        self.connect()
        self.models_path = 'wafer/models/'

    def connect(self):
        """
        Connects to the s3 bucket
        :return:
        """
        try:
            self.session = Session(aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
            self.s3_resource = self.session.resource('s3')
            self.bucket = self.s3_resource.Bucket(self.bucket_name)
        except Exception as e:
            raise Exception('Some Error occurred while connecting to the cloud storage')
        return

    def download_all(self, to_directory):
        """
        Download all the training files to specified directory
        :param to_directory: directory path to store the downloaded files
        :return:
        """
        for i, file in enumerate(self.bucket.objects.all()):
            # bucket.object.all() returns objects with bucket_name, key
            s3_filepath = file.key  # key --> filepath
            s3_filename = s3_filepath.split('/')[-1]
            self.bucket.download_file(s3_filepath, to_directory + s3_filename)
        return

    def upload_from_directory(self, from_directory, to_directory):
        """
        Uploads all the files in a folder in local directory to s3 directory
        :param from_directory: local directory path
        :param to_directory: s3 directory path
        :return:
        """
        if not str(to_directory).endswith('/'):     # if the path is not provided correctly append a /
            to_directory = to_directory + '/'

        # upload all the files in the specified directory
        for file in os.listdir(from_directory):
            if str(file).endswith('.csv'):
                self.bucket.upload_file(from_directory + str(file), to_directory + str(file))
        return

    def read_data(self, path):
        """
        Reads the data file using pandas
        :param path: complete path to the s3 file
        :return:
        """
        try:
            resource = self.s3_resource
            s3_object = resource.Object(self.bucket_name, path)
            object_response = s3_object.get()
            data = pd.read_csv(object_response['Body'])
            return data
        except Exception as e:
            raise Exception("Error while reading the file stored in path : {}".format(path))

    def get_file_names(self, directory):
        """
        Returns a list of names of all the files in a S3 Folder
        :param directory: path of the s3 Folder which contains all the files
        :return: List of all the Filenames
        """
        filename_list = []
        for objects in self.bucket.objects.filter(Prefix=directory):
            filename_list.append(str(objects.key).split('/')[-1])
        return filename_list

    def save_model(self, model, filename):
        pickle_object = pickle.dumps(model)
        self.s3_resource.Object(self.bucket_name, self.models_path + str(filename)).put(Body=pickle_object)
        return

    def load_model(self, filename):
        """
        Loads Models from cloud saved in directory - wafer/models/<filename>
        :param filename:
        :return:
        """
        model_object = self.s3_resource.Object(self.bucket_name, self.models_path + str(filename)).get()['Body'].read()
        model = pickle.loads(model_object)
        return model

    def write_json(self, json_file, filename):
        json_object = json.dumps(json_file)
        self.s3_resource.Object(self.bucket_name, self.models_path + str(filename)).put(Body=json_object)
        return

    def load_json(self, filename):
        json_object = self.s3_resource.Object(self.bucket_name, self.models_path + str(filename)).get()['Body'].read()
        json_file = json.loads(json_object)
        return json_file
