from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.component.data_ingestion import DataIngestion
from finance_complaint.component.data_validation import DataValidation



c=FinanceConfig()

di=DataIngestion(data_ingestion_config=c.get_data_ingestion_config())

dv=DataValidation(data_validation_config=c.get_data_validation_config(), 
                    data_ingestion_artifact=di.initiate_data_ingestion())

print(dv.initiate_data_validation())