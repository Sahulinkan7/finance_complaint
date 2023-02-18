from collection import namedtuple

TrainingPipelineConfig=namedtuple("TrainingPipelineConfig",
                                    ["pipelinename","artifact_dir"])

DataIngestionConfig=namedtuple("DataIngestionConfig",
                                ["from_date","to_date","data_ingestion_dir","download_dir",
                                "file_name","feature_store_dir","failed_dir","metadata_file_path"
                                "datasource_url"])

