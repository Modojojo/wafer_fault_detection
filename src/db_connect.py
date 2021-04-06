import pymongo.errors
import pandas as pd
from datetime import datetime, date
import os

DB_NAME = os.environ.get("WAFER_DB_NAME")
DB_KEY = os.environ.get("DB_KEY")

datetime_lib = datetime
date_lib = date

class DbConnector:
    def __init__(self):
        self.TRAINING_COLLECTION_NAME = 'training_data'
        self.ERRORED_COLLECTION_NAME = 'errored_data'
        self.db = None
        self.client = None
        self.database = None
        self.db_name = DB_NAME
        self.db_key = DB_KEY
        self.datetime_lib = datetime_lib
        self.date_lib = date_lib
        self.connect()

    def connect(self):
        try:
            self.client = pymongo.MongoClient(f"mongodb+srv://mododb:{self.db_key}@testcluster.mbnqg.mongodb.net/test")
            self.database = self.client[self.db_name]
        except Exception as e:
            return e

    def close(self):
        self.client.close()

    def check_connection(self):
        if self.database is None:
            raise pymongo.errors.ServerSelectionTimeoutError

    def get_date(self):
        datenow = self.date_lib.today()
        return str(datenow)

    def get_time(self):
        timenow = self.datetime_lib.now()
        current_time = timenow.strftime("%H:%M:%S")
        return str(current_time)

    def insert_training_data(self, file):
        collection = self.database['training_data']
        try:
            collection.insert_many(file.to_dict('records'))
        except Exception as e:
            print(str("Error Occured while inserting the records into the database"))

    def clear_training_folder(self):
        collection = self.database['training_data']
        collection.drop()

    def fetch_training_data(self):
        collection = self.database['training_data']
        data = pd.DataFrame.from_records(collection.find({}, {'_id':0}))
        return data

    def insert_errored_data(self, file):
        collection = self.database['bad_data']
        try:
            collection.insert_many(file.to_dict('records'))
        except Exception as e:
            print(str("Error Occurred while inserting the records into the database"))

    def clear_bad_data_folder(self):
        collection = self.database['bad_data']
        collection.drop()

    def fetch_bad_data(self):
        collection = self.database['bad_data']
        data = pd.DataFrame.from_records(collection.find({}, {'_id': 0}))
        return data

    # SAVE TRAINING METRICS
    def save_metrics(self, data):
        collection = self.database['METRICS']
        timenow = self.get_time()
        datenow = self.get_date()
        data['date'] = datenow
        data['timestamp'] = timenow
        collection.insert(data)

    def fetch_metrics(self):
        collection = self.database['METRICS']
        return_data = []
        data = collection.find({}, {'_id': 0})
        for doc in data:
            return_data.append(doc)
        return return_data

    # Prediction Functionality

    def insert_predictions(self, predictions):
        collection = self.database['PREDICTIONS']
        try:
            for i in predictions:
                predictions[i] = str(predictions[i])
            timenow = self.get_time()
            datenow = self.get_date()
            predictions['date'] = datenow
            predictions['timestamp'] = timenow
            collection.insert(predictions)
        except Exception as e:
            print('DB.INSERT_PREDICTIONS | ' + str(e))

    def insert_prediction_data(self, file):
        collection = self.database['prediction_data']
        try:
            collection.insert_many(file.to_dict('records'))
        except Exception as e:
            print(str("Error Occured while inserting the records into the database"))

    def fetch_prediction_data(self):
        collection = self.database['prediction_data']
        data = pd.DataFrame.from_records(collection.find({}, {'_id':0}))
        return data

    def clear_prediction_folder(self):
        collection = self.database['prediction_data']
        collection.drop()

    def insert_errored_prediction_data(self, file):
        collection = self.database['bad_data_prediction']
        try:
            collection.insert_many(file.to_dict('records'))
        except Exception as e:
            print(str("Error Occurred while inserting the records into the database"))

    def clear_bad_data_prediction_folder(self):
        collection = self.database['bad_data_prediction']
        collection.drop()

    def fetch_bad_data_prediction(self):
        collection = self.database['bad_data_prediction']
        data = pd.DataFrame.from_records(collection.find({}, {'_id': 0}))
        return data