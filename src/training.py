from src.preprocessor import Preprocessor
from src.custom_exceptions import PreProcessException
from src.db_connect import DbConnector
from src.create_clusters import Cluster
from src.models_utils import Models
from sklearn.model_selection import train_test_split


class Training:
    def __init__(self, logger_object, cloud_connect_object):
        self.logger = logger_object
        self.cloud = cloud_connect_object
        self.db = DbConnector()
        self.models_folder = None
        self.features = None
        self.labels = None
        self.models_save_extn = '.pkl'
        self.training_schema_dict = {}

    def create_clusters(self, features):
        """
        Creates Clusters based on training data provided
        :param features: dataframe containing training data
        :return: dataframe containing features and a cluster number column
        """
        cluster = Cluster(self.logger, self.cloud)
        cluster_labels = cluster.create_cluster(features)
        features['cluster'] = cluster_labels
        self.features = features
        return features

    def create_models(self, features, labels):
        """
        Create and Train the Best Possible Model for Identified clusters.
        :param features: Dataframe containing the training features including the Cluster number column
        :param labels: Series containing the Training Labels
        :return: None
        """
        self.training_schema_dict['valid_columns'] = features.drop('cluster', axis=1).columns.tolist()
        features['class'] = labels
        for i in features['cluster'].unique().tolist():
            data = features[features['cluster'] == i].drop('cluster', axis=1)
            labels_train = data['class']
            data_train = data.drop('class', axis=1)
            x_train, x_test, y_train, y_test = train_test_split(data_train, labels_train, test_size=1/3)
            models = Models(x_train, y_train, x_test, y_test, self.logger)
            self.logger.log_training('TRAINING STARTED FOR CLUSTER : {}'.format(i))
            model_retvar = models.get_best_model()
            if model_retvar is not False:
                (model, model_name, metrics) = model_retvar
                model_save_path = str(i) + '_' + str(model_name) + '/' + 'model' + str(self.models_save_extn)
                self.cloud.save_model(model, model_save_path)
                self.logger.log_training('TRAINING COMPLETED FOR CLUSTER : {}'.format(i))
                self.training_schema_dict[i] = model_save_path
                self.db.save_metrics(metrics)
        print('TRAINING SCHEMA DICT', self.training_schema_dict)
        self.cloud.write_json(self.training_schema_dict, 'models_schema.json')
        self.logger.pipeline_logs('TRAINING_SCHEMA_FILE_UPLOADED')

    def preprocess(self, dataframe):
        """
        Preprocess the data and gets it ready for Clustering and Training
        :param dataframe: DataFrame containing Features and labels.
        :return: Features (DataFrame) and Labels (Series)
        """
        preProc = Preprocessor(self.logger, self.cloud)
        dataframe = preProc.drop_id(dataframe)
        if dataframe is not False:
            features_labels_tuple = preProc.create_features_and_labels(dataframe)
            if features_labels_tuple is not False:
                features, labels = features_labels_tuple
                features = preProc.handle_null_values(features)
                if features is not False:
                    features = preProc.drop_cols_with_zero_dev(features)
                    if features is not False:
                        self.features = features
                        self.labels = labels
                        return features, labels
                    else:
                        raise PreProcessException
                else:
                    raise PreProcessException
            else:
                raise PreProcessException
        else:
            raise PreProcessException

    def fetch_data(self):
        """
        Fetches Training data from Database (MongoDB)
        :return: Dataframe containing the data
        """
        dataframe = self.db.fetch_training_data()
        return dataframe

    # pipeline testing Function
    def test(self):
        data = self.fetch_data()
        features, labels = self.preprocess(data)
        features = self.create_clusters(features)
        self.create_models(features, labels)
        
    def train(self):
        """
        Main Function that Performs the Model Training Operations.
        :return: 
        """
        data = self.fetch_data()
        features, labels = self.preprocess(data)
        features = self.create_clusters(features)
        self.create_models(features, labels)
        self.logger.pipeline_logs('---=== TRAINING PROCESS COMPLETED===---')
