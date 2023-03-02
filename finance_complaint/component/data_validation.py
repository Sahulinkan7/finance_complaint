from finance_complaint.constant.training_pipeline_config import *
from finance_complaint.config.pipeline.training import DataValidationConfig
from finance_complaint.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
import os,sys
from finance_complaint.logger import logging
from finance_complaint.exception import FinanceException
from pyspark.sql import DataFrame
from finance_complaint.config.spark_manager import spark_session
from collections import namedtuple
from typing import Dict,List
from finance_complaint.entity.schema import FinanceDataSchema
from pyspark.sql.functions import lit,col

ERROR_MESSAGE="error_msg"

Missingreport=namedtuple("Missingreport",["total_row","missing_row","missing_percentage"])


class DataValidation(FinanceDataSchema):
    def __init__(self,data_validation_config:DataValidationConfig,
                data_ingestion_artifact:DataIngestionArtifact,
                schema=FinanceDataSchema()):
        try:
            self.data_validation_config=data_validation_config
            self.data_ingestion_artifact=data_ingestion_artifact
            self.schema=schema

        except Exception as e:
            raise FinanceException(e, sys) from e
    
    def read_data(self)->DataFrame:
        try:
            dataframe: DataFrame=spark_session.read.parquet(self.data_ingestion_artifact.feature_store_file_path)
            logging.info(f"Data frame is created using file : {self.data_ingestion_artifact.feature_store_file_path}")
            logging.info(f"Number of rows : {dataframe.count()} and number of columns : {len(dataframe.columns)}")
            # dataframe, _=dataframe.randomSplit([0.001,0.999])
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys) from e

    @staticmethod
    def get_missing_report(dataframe:DataFrame)->Dict[str,Missingreport]:
        try:
            missing_report: Dict[str:Missingreport]=dict()
            logging.info(f"Preparing missing report for each column")
            number_of_row=dataframe.count()

            for column in dataframe.columns:
                missing_row=dataframe.filter(f"{column} is null").count()
                missing_percentage=(missing_row*100)/number_of_row
                missing_report[column]=Missingreport(total_row=number_of_row,
                                                    missing_row=missing_row,
                                                    missing_percentage=missing_percentage)

            logging.info(f"Missing report prepared : {missing_report}")
            return missing_report
        except Exception as e:
            raise FinanceException(e, sys) from e

    def get_unwanted_and_high_missing_values_columns(self,dataframe:DataFrame,threshold:float=0.45)->List[str]:
        try:
            missing_report:Dict[str,MissingReport]=self.get_missing_report(dataframe=dataframe)

            unwanted_column:List[str]=self.schema.unwanted_columns
            for column in missing_report:
                if missing_report[column].missing_percentage>(threshold*100):
                    unwanted_column.append(column)
                    logging.info(f"Missing report {column}:[{missing_report[column]}]")
            unwanted_column=list(set(unwanted_column))
            return unwanted_column

        except Exception as e:
            raise FinanceException(e, sys) from e
    
    def drop_unwanted_columns(self,dataframe:DataFrame)->DataFrame:
        try:
            unwanted_columns:List=self.get_unwanted_and_high_missing_values_columns(dataframe=dataframe)
            logging.info(f"Dropping feature : {','.join(unwanted_columns)}")

            unwanted_dataframe:DataFrame=dataframe.select(unwanted_columns)
            unwanted_dataframe=unwanted_dataframe.withColumn(ERROR_MESSAGE,lit("contains many missing values"))

            rejected_dir=os.path.join(self.data_validation_config.rejected_data_dir,"missing_data")
            os.makedirs(rejected_dir,exist_ok=True)
            file_path=os.path.join(rejected_dir,self.data_validation_config.file_name)

            logging.info(f"writing dropped columns into file : [{file_path}]")
            unwanted_dataframe.write.mode("append").parquet(file_path)
            dataframe:DataFrame=dataframe.drop(*unwanted_columns)
            logging.info(f"Remaining number of columns : [{dataframe.columns}]")
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys) from e

    @staticmethod
    def get_unique_values_of_each_column(dataframe:DataFrame)->None:
        try:
            for column in dataframe.columns:
                n_unique: int=dataframe.select(col(column)).distinct().count()
                n_missing: int=dataframe.filter(col(column).isNull()).count()
                missing_percentage:float=(n_missing*100)/dataframe.count()
                logging.info(f"Column: {column} contains {n_unique} value and missing perc: {missing_percentage} %.")
        except Exception as e:
            raise FinanceException(e, sys) from e
    
    def is_required_columns_exist(self,dataframe:DataFrame):
        try:
            columns=list(filter(lambda x: x in self.schema.required_columns,dataframe.columns))
            logging.info(f"Found columns are :{columns}")
            logging.info(f"expected columns: {self.schema.required_columns}")

            if len(columns)!=len(self.schema.required_columns):
                raise Exception(f"""Required column missing :
                                Expected columns: {self.schema.required_columns}
                                Found columns : {columns}""")
            
        except Exception as e:
            raise FinanceException(e, sys) from e
    
    
    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            logging.info(f"Initiating data validation")
            dataframe:DataFrame=self.read_data()

            logging.info(f"Dropping unwanted columns")
            dataframe:DataFrame = self.drop_unwanted_columns(dataframe=dataframe)

            self.is_required_columns_exist(dataframe=dataframe)

            logging.info(f"Saving validated data")
            logging.info(f"Row: [{dataframe.count()}] column: [{len(dataframe.columns)}]")
            logging.info(f"Expected columns : {self.schema.required_columns} present columns: {dataframe.columns}")

            os.makedirs(self.data_validation_config.accepted_data_dir,exist_ok=True)
            accepted_file_path=os.path.join(self.data_validation_config.accepted_data_dir,
                                            self.data_validation_config.file_name)

            dataframe.write.parquet(accepted_file_path)

            data_validation_artifact=DataValidationArtifact(accepted_file_path=accepted_file_path,
                                            rejected_dir=self.data_validation_config.rejected_data_dir)

            logging.info(f"Data Validation Artifact: {data_validation_artifact}")

            return data_validation_artifact

        except Exception as e:
            raise FinanceException(e, sys) from e