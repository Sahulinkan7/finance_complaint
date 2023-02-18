from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
import os,sys
from finance_complaint.entity.config_entity import DataIngestionConfig
from finance_complaint.entity.artifact_entity import DataIngestionArtifact

class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        try:
            pass
        except Exception as e:
            raise FinanceException(e, sys) from e