from src.cloud_connect import Cloud
from src.custom_logger import Logger
from src.db_connect import DbConnector
from src.prepare_data import PrepareData
from src.prepare_prediction_data import PreparePredictionData
from src.training import Training
from src.prediction import Predictor
from flask import Flask, render_template, request
import pymongo.errors
import threading
import time
import os

TOTAL_FILES = None

params_path = 'params.yaml'
webapp_root = 'webapp'

static_dir = os.path.join(webapp_root, "static", "css")
template_dir = os.path.join(webapp_root, "templates")

app = Flask(__name__, static_folder=static_dir, template_folder=template_dir)


@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')


@app.route('/train', methods=['GET'])
def start_training():
    try:
        logger = Logger()
        logger.check_connection()
        cloud = Cloud()

        logger.move_logs_to_hist()

        # prepare Data
        logger.pipeline_logs('---=== PROCESS STARTED ===---')
        prepare_data = PrepareData('wafer/data/training/', logger, cloud)
        prepare_data.prepare()

        # start Training
        logger.log_training('---=== TRAINING PROCESS STARTED ===---')
        trainer = Training(logger_object=logger, cloud_connect_object=cloud)
        trainer.train()

        logger.close()

        return render_template('training_completed.html')
    except pymongo.errors.ServerSelectionTimeoutError as e:
        return render_template('404.html', message="Please ask admin to check the database connection")
    except Exception as e:
        logger.pipeline_logs('CRITICAL : Cannot Proceed wth Training, Please check the Logs')
        logger.close()
        logger.pipeline_logs(str(e))
        return render_template('404.html', message=str(e))


@app.route('/predict', methods=['GET'])
def predict():
    try:
        logger = Logger()
        logger.check_connection()
        logger.move_prediction_logs_to_hist()
        cloud = Cloud()
        db = DbConnector()

        prepare_data = PreparePredictionData(logger, cloud, db)
        prepare_data.prepare()

        predictor = Predictor(logger, cloud, db)
        predictor.predict()

        logger.pipeline_logs('---=== PROCESS COMPLETED : PREDICTION ===---')
        logger.close()
        db.close()

        return render_template('prediction_completed.html')
    except pymongo.errors.ServerSelectionTimeoutError:
        return render_template('404.html', message="Please ask admin to check the database connection")
    except Exception as e:
        logger.pipeline_logs('CRITICAL : Cannot Proceed wth Prediction, Please check the Logs')
        logger.close()
        return render_template('404.html', message="Cannot Proceed due to some Error, Please check the logs")


@app.route('/metrics', methods=['GET'])
def get_metrics():
    try:
        db = DbConnector()
        db.check_connection()
        metrics = db.fetch_metrics()
        db.close()
        for metric in metrics:
            for i in metric:
                if len(metric[i]) > 5 and i != 'model':
                    metric[i] = metric[i][:5]
        return render_template('metrics.html', metrics=metrics)

    except pymongo.errors.ServerSelectionTimeoutError:
        return render_template('404.html', message='Please ask admin to check the database connection')

    except Exception as e:
        print(e)
        return render_template('404.html', message=str(e))


@app.route('/logs', methods=['POST'])
def get_logs():
    try:
        log_collection_name = None
        log_type = request.form.get('logs')
        if log_type == 'process':
            log_collection_name = 'process'
        elif log_type == 'training':
            log_collection_name = 'training'
        elif log_type == 'file_validation':
            log_collection_name = 'file_validation'
        elif log_type == 'prediction':
            log_collection_name = 'prediction'

        logger = Logger()
        logs = logger.export_logs(log_collection_name)
        logger.close()
        return render_template('logs.html', logs=logs)

    except pymongo.errors.ServerSelectionTimeoutError:
        return render_template('404.html', message='Please ask admin to check the database connection')

    except Exception as e:
        return render_template('404.html', message=str(e))


def watcher():
    """
    File Watcher, Triggers prediction whenever a new file has been added to server, checks every 2 minutes
    :return:
    """
    # checking if logger is working or not
    # if not then handle exception otherwise watcher thread would stop
    create_logs = True
    try:
        logger = Logger()
        logger.pipeline_logs("Watcher Started")
    except pymongo.errors.ServerSelectionTimeoutError:
        create_logs = False
    except Exception as e:
        print(e)
        create_logs = False

    cloud = Cloud()
    db = DbConnector()
    current_file_count = None
    total_file_count = None
    print('WATCHER THREAD STARTED')
    while True:
        if total_file_count is None or current_file_count is None:
            filenames = cloud.get_file_names('wafer/data/prediction/')
            filecount = len(filenames)
            current_file_count = filecount
            total_file_count = filecount
        elif current_file_count < total_file_count:
            total_file_count = current_file_count
        else:
            time.sleep(120)
            filenames = cloud.get_file_names('wafer/data/prediction/')
            current_file_count = len(filenames)
            if current_file_count != total_file_count and current_file_count != 0:
                print('TRIGGERED PREDICTION')
                if create_logs is True:
                    logger.pipeline_logs('TRIGGER : PREDICTION TRIGGERED : New Files Found For Prediction')

                prepare_data = PreparePredictionData(logger, cloud, db)
                prepare_data.prepare()

                predictor = Predictor(logger, cloud, db)
                predictor.predict()
                if create_logs is True:
                    logger.pipeline_logs('TRIGGER : PROCESS COMPLETED :  PREDICTIONS SAVED TO DATABASE ')
                total_file_count = current_file_count


if __name__ == '__main__':
    watcher_thread = threading.Thread(target=watcher)
    watcher_thread.start()
    app.run()
