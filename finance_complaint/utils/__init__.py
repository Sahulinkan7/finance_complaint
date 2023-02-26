from finance_complaint.exception import FinanceException
import yaml
import os

def write_yaml_file(file_path:str,data:dict=None):
    """
    create yaml file
    file_path:str
    data: dict type
    """
    try:
        os.makedirs(os.path.dirname(file_path),exist_ok=True)
        with open(file_path,"w") as yaml_file:
            yaml.dump(data, yaml_file)
    except Exception as e:
        raise FinanceException(e, sys) from e

def read_yaml_file(file_path:str)->dict:
    """
    read yaml file and returns the content as dictionary
    file_path:str
    """
    try:
        with open(file_path,'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise FinanceException(e, sys) from e

