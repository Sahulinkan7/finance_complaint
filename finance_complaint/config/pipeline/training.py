from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
import os,sys
from datetime import datetime
from finance_complaint.entity.config_entity import DataIngestionConfig,TrainingPipelineConfig
from finance_complaint.constant.training_pipeline_config import *
from finance_complaint.constant import TIMESTAMP
from finance_complaint.entity.metadata_entity import DataIngestionMetadata


class FinanceConfig:
    def __init__(self,pipeline_name=PIPELINE_NAME,timestamp=TIMESTAMP):
        try:

            self.timestamp=timestamp
            self.pipeline_name=pipeline_name
            self.training_pipeline_config=self.get_training_pipeline_config()

        except Exception as e:
            raise FinanceException(e,sys) from e

    def get_training_pipeline_config(self)->TrainingPipelineConfig:
        try:
            artifact_dir=PIPELINE_ARTIFACT_DIR
            training_pipeline_config=TrainingPipelineConfig(pipeline_name=self.pipeline_name,
                                                    artifact_dir=artifact_dir)

            logging.info(f"Training pipeline configuration : {training_pipeline_config}")
            return training_pipeline_config

        except Exception as e:
            raise FinanceException(e,sys) from e

    def get_data_ingestion_config(self,from_date=DATA_INGESTION_MIN_START_DATE,to_date=None)->DataIngestionConfig:
        try:
            min_start_date=datetime.strptime(DATA_INGESTION_MIN_START_DATE,"%Y-%m-%d")
            from_date_obj=datetime.strptime(from_date,"%Y-%m-%d")

            if from_date_obj<min_start_date:
                from_date=DATA_INGESTION_MIN_START_DATE
            if to_date==None:
                to_date=datetime.now().strftime("%Y-%m-%d")
            
            data_ingestion_master_dir=os.path.join(self.training_pipeline_config.artifact_dir,
                                                    DATA_INGESTION_DIR)
            #time based dir for each run
            data_ingestion_dir=os.path.join(data_ingestion_master_dir,self.timestamp)

            metadata_file_path=os.path.join(data_ingestion_master_dir,DATA_INGESTION_METADATA_FILE_NAME)
            logging.info(f"metadata file path : {metadata_file_path}")

            data_ingestion_metadata=DataIngestionMetadata(metadata_file_path=metadata_file_path)

            if data_ingestion_metadata.is_metadata_file_present:
                metadata_info=data_ingestion_metadata.get_metadata_info()
                from_date=metadata_info.to_date

            data_ingestion_config=DataIngestionConfig(from_date=from_date,
                                                    to_date=to_date,
                                                    data_ingestion_dir=data_ingestion_dir,
                                                    download_dir=os.path.join(data_ingestion_dir,DATA_INGESTION_DOWNLOADED_DATA_DIR),
                                                    file_name=DATA_INGESTION_FILE_NAME,
                                                    feature_store_dir=os.path.join(data_ingestion_master_dir,DATA_INGESTION_FEATURE_STORE_DIR),
                                                    failed_dir=os.path.join(data_ingestion_dir,DATA_INGESTION_FAILED_DIR),
                                                    metadata_file_path=metadata_file_path,
                                                    datasource_url=DATA_INGESTION_DATA_SOURCE_URL
                                                    )
            logging.info(f"Data Ingestion Config {data_ingestion_config}")
            return data_ingestion_config

        except Exception as e:
            raise FinanceException(e, sys) from e

    def get_data_validation_config(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys) from e

    def get_data_transformation_config(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys) from e

    def get_model_trainer_config(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys) from e

    def get_model_evaluation_config(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys) from e

    def get_model_pusher_config(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e, sys) from e