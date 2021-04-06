import pandas as pd


class PredictionPreprocessor:
    def __init__(self, logger_object, cloud_object):
        self.logger = logger_object
        self.cloud = cloud_object
        self.imputer_save_path = 'imputer/KNNImputer.pkl'

    def get_valid_columns(self, dataframe, schema):
        try:
            columns = schema['valid_columns']
            dataframe = dataframe[columns]
            return dataframe
        except Exception as e:
            self.logger.prediction_pipeline_logs('PREDICTION_PREPROCESSING : Error while creating valid columns')
            return False

    def load_imputer(self):
        try:
            imputer = self.cloud.load_model(self.imputer_save_path)
            return imputer
        except Exception as e:
            self.logger.prediction_pipeline_logs('PREDICTION_PREPROCESS : Error While Loading Imputer')
            raise Exception('Imputer loading Failed')

    def handle_null_values(self, dataframe):
        try:
            imputer = self.load_imputer()
            dataframe = imputer.transform(dataframe)
            return dataframe
        except Exception as e:
            self.logger.prediction_pipeline_logs('PREDICTION_PREPROCESS : Error While imputing null values')
            return False

    def preprocess(self, dataframe, schema):
        dataframe_new = self.handle_null_values(dataframe)
        if dataframe is not False:
            dataframe = pd.DataFrame(data=dataframe_new, columns=dataframe.columns)
            dataframe = self.get_valid_columns(dataframe, schema)
            if dataframe is not False:
                return dataframe
            else:
                return False
        else:
            return False
