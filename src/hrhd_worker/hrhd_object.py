

import os
from configparser import ConfigParser
import sys
sys.path.append(os.getcwd()[:os.getcwd().find("HRHD_Worker")+len("HRHD_Worker")])


class HRHDObjects:

    parser = ConfigParser()
    os.environ['HRHD_CONFIG'] = str(os.getcwd()[:os.getcwd().find("HRHD_Worker")+len("HRHD_Worker")])+"/config/config.ini"
    parser.read(os.getenv("HRHD_CONFIG"))
    
    

    @staticmethod
    def get_with_base_path(head, key):
        return str(os.getcwd()[:os.getcwd().find("HRHD_Worker")+len("HRHD_Worker/")])+HRHDObjects.parser.get(head, key)

    @staticmethod
    def get_value(head, key):
        return HRHDObjects.parser.get(head, key)
