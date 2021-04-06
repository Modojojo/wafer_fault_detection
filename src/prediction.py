from src.create_clusters import Cluster
from src.prediction_preprocessor import PredictionPreprocessor

class Predictor:
    def __init__(self, logger_object, cloud_connect_object, db_connect_object):
        self.logger = logger_object
        self.cloud = cloud_connect_object
        self.db = db_connect_object
        self.clustering_model_filepath = None
        self.dataframe = None

    def fetch_data(self):
        dataframe = self.db.fetch_prediction_data()
        self.dataframe = dataframe
        return dataframe

    def preprocess(self, dataframe, prediction_schema):
        wafer_ids = dataframe['wafer_id']
        dataframe = dataframe.drop('wafer_id', axis=1)
        predProcessor = PredictionPreprocessor(self.logger, self.cloud)
        dataframe = predProcessor.preprocess(dataframe, prediction_schema)
        if dataframe is not False:
            dataframe.insert(0, 'wafer_id', wafer_ids)
            return dataframe
        else:
            raise Exception('PredictionPreprocessingFailed')

    def predict_clusters(self, dataframe):
        cluster = Cluster(self.logger, self.cloud)
        cluster_predictions = cluster.predict_clusters(dataframe)
        return cluster_predictions

    def predict_class(self, dataframe, cluster_number, prediction_schema):
        model_path = prediction_schema[str(cluster_number)]
        model = self.cloud.load_model(model_path)
        predictions = model.predict(dataframe)
        return predictions

    def predict(self):
        try:
            self.logger.prediction_pipeline_logs('---=== PROCESS STARTED : PREDICTION ===---')
            final_predictions_dict = {}
            dataframe = self.fetch_data()
            self.logger.prediction_pipeline_logs('PREDICTION_PROCESS : Data Fetched Successfully')
            prediction_schema = self.cloud.load_json('models_schema.json')
            self.logger.prediction_pipeline_logs('PREDICTION_PROCESS : Prediction Schema Loaded')
            dataframe = self.preprocess(dataframe, prediction_schema)
            self.logger.prediction_pipeline_logs('PREDICTION_PROCESS : Preprocessing Completed Successfully')
            cluster_predictions = self.predict_clusters(dataframe.drop('wafer_id', axis=1))
            if cluster_predictions is not False:
                self.logger.prediction_pipeline_logs('PREDICTION_PROCESS : Cluster Prediction Completed Successfully')
                dataframe['cluster'] = cluster_predictions
                for cluster_no in dataframe['cluster'].unique().tolist():
                    new_df = dataframe[dataframe['cluster'] == cluster_no]
                    predictions = self.predict_class(new_df.drop(['cluster', 'wafer_id'], axis=1),
                                                     cluster_no,
                                                     prediction_schema)
                    if predictions is not False:
                        self.logger.prediction_pipeline_logs('PREDICTION_PROCESS : Classification Completed Successfully for cluster : {}'.format(cluster_no))
                        wafer_id_list = new_df['wafer_id'].tolist()
                        for i in range(len(wafer_id_list)):
                            wafer_id = wafer_id_list[i]
                            final_predictions_dict[wafer_id] = predictions[i]
                    else:
                        self.logger.prediction_pipeline_logs(
                            'PREDICTION_PROCESS : FAILED : Failed to predict classes for cluster : {}'.format(cluster_no))
                self.db.insert_predictions(final_predictions_dict)
                self.logger.prediction_pipeline_logs('PREDICTION_PROCESS : COMPLETED : saving predictions')
                return final_predictions_dict
        except Exception as e:
            self.logger.prediction_pipeline_logs('PREDICTION : CRITICAL_ERROR : Prediction Failed')
            return False
