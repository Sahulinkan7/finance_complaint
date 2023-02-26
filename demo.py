from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.component.data_ingestion import DataIngestion



c=FinanceConfig()
iconfig=c.get_data_ingestion_config()

di=DataIngestion(data_ingestion_config=iconfig)
print(di.initiate_data_ingestion())