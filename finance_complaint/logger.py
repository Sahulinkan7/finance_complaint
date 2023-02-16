from .constant import TIMESTAMP
import logging
import os
LOG_DIR="logs"
LOG_FILE_NAME=f"log_{TIMESTAMP}.log"
LOG_FILE_PATH=os.path.join(LOG_DIR,LOG_FILE_NAME)

logging.basicConfig(filename=LOG_FILE_PATH,
                    filemode="w",
                    level=logging.INFO,
                    format='[%(asctime)s] \t%(levelname)s \t%(lineno)d \t%(filename)s \t%(funcName)s() \t%(message)s'
                    )
