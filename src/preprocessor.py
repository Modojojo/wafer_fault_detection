from sklearn.impute import KNNImputer
import numpy as np
import pandas as pd


class Preprocessor:
    def __init__(self, logger_object, cloud_connect_object):
        self.logger = logger_object
        self.cloud = cloud_connect_object
        self.imputer_save_path = 'imputer/KNNImputer.pkl'
        self.dropped_columns = []

    def drop_id(self, dataframe):
        """
        Removes the Index/Id column from the Dataframe
        :param dataframe:
        :return:
        """
        try:
            dataframe = dataframe.drop(['wafer_id'], axis=1)
            return dataframe
        except Exception as e:
            self.logger.pipeline_logs('Error while removing columns : wafer_id columns might be missing from data')
            return False

    def create_features_and_labels(self, dataframe):
        """
        Separates the features and the labels columns of a Dataframe
        :param dataframe:
        :return:
        """
        try:
            labels = dataframe['class']
            features = dataframe.drop(['class'], axis=1)
            return features, labels
        except Exception as e:
            self.logger.pipeline_logs('Error While creating Features and Labels')
            return False

    def handle_null_values(self, dataframe):
        """
        Imputes the NaN values from the Columns using KNN-Imputer
        :param dataframe:
        :return: Processed Dataframe if no Errors are encountered, else, False
        """
        try:
            if dataframe.isna().sum().sum() > 0:
                imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values=np.nan)
                new_dataframe = imputer.fit_transform(dataframe)
                dataframe = pd.DataFrame(data=new_dataframe, columns=dataframe.columns)
                self.cloud.save_model(imputer, self.imputer_save_path)
            return dataframe
        except Exception as e:
            self.logger.pipeline_logs('Error While imputing null Values')
            return False

    def drop_cols_with_zero_dev(self, dataframe):
        """
        Drops the columns that have a standard Deviation of 0
        :param dataframe:
        :return: Processed Dataframe
        """
        try:
            to_drop = []
            std_devs = np.std(dataframe)
            to_drop = std_devs[std_devs == 0.0].index.tolist()
            if len(to_drop) > 0:
                dataframe = dataframe.drop(to_drop, axis=1)
                self.logger.pipeline_logs('Dropped Columns with STD DEV = 0 : {}'.format(to_drop))
                self.dropped_columns = to_drop
                #self.cloud.write_json(self.dropped_columns, 'columns_list.json')
            return dataframe
        except Exception as e:
            self.logger.pipeline_logs('Error While dropping columns with Zero STD DEV')
            return False






