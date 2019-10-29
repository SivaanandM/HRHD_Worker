import logging
import os
import sys
sys.path.append(os.getcwd()[:os.getcwd().find("HRHD_Worker")+len("HRHD_Worker")])
import shutil
import traceback
from src.hrhd_worker.hrhd_object import HRHDObjects as HRHD_Obj


def setup_custom_logger(name):
    try:
        if not os.path.exists(HRHD_Obj.parser.get('common', 'log_path')):
            os.makedirs(HRHD_Obj.parser.get('common', 'log_path'))
        else:
            shutil.rmtree(HRHD_Obj.parser.get('common', 'log_path'))
            os.makedirs(HRHD_Obj.parser.get('common', 'log_path'))
        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
        if HRHD_Obj.parser.get('common', 'log_level').lower() == "info":
            log_level = logging.INFO
        elif HRHD_Obj.parser.get('common', 'log_level').lower() == "debug":
            log_level = logging.DEBUG
        elif HRHD_Obj.parser.get('common', 'log_level').lower() == "error":
            log_level = logging.ERROR
        elif HRHD_Obj.parser.get('common', 'log_level').lower() == "warn":
            log_level = logging.WARN
        else:
            log_level = logging.INFO
        logfile = HRHD_Obj.parser.get('common', 'log_path')+os.sep+"hrhd_worker.log"
        handler = logging.FileHandler(logfile)
        handler.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.setLevel(log_level)
        logger.addHandler(handler)
        return logger
    except Exception as ex:
        logger.error(ex)
        logger.error(traceback.format_exc())
    return None
