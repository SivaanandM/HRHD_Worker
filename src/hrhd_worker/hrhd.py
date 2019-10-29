import sys
import os
import argparse
import json
import random
import subprocess
import traceback
sys.path.append(os.getcwd()[:os.getcwd().find("HRHD_Worker")+len("HRHD_Worker/")])

from src.hrhd_worker.hrhd_object import HRHDObjects as hrhdObj
from src.libs.firebase_utils import FireBaseUtils
from src.loghandler import log

logger = log.setup_custom_logger('hrhd')
nse_dict_list = None

class Hrhd:

    def __init__(self):
        logger.info("** Initiating....")

    @staticmethod
    def nifty_list(list_config_name):
        try:
            with open(hrhdObj.get_with_base_path("common", list_config_name), 'r') as json_file:
                nse_dict = json.load(json_file)
            return nse_dict
        except Exception as ex:
            logger.error(traceback.format_exc())

    @staticmethod
    def get_ib_symbol_from_map(nse_symbol):
        try:
            map_json = json.loads(open(str(hrhdObj.get_with_base_path('common', 'ib_nse_map'))).read())
            for sym in map_json:
                if sym['NSE_Symbol'] == nse_symbol:
                    return sym['IB_Symbol']
            return nse_symbol
        except Exception as ex:
            logger.error("No mapping value found for NSE Symbol:%s" % nse_symbol)
            logger.error(traceback.format_exc())


    @staticmethod
    def worker_as_process_tick_data(gateway_ip, symbol, date, cid):
        try:
            print("-------------------------------------------")
            print("Data Recordings started for -", symbol)
            os.chdir(os.getcwd())
            print(os.getcwd())
            subprocess.call(["python3 hrhd_worker.py -cid "+str(cid)+" -ip "+gateway_ip+" -p 4002 -s " + symbol + " -d " + date + " -st STK"],
                shell=True)
            print("-------------------------------------------")

        except Exception as ex:
            logger.error(traceback.format_exc())

    @staticmethod
    def worker_as_process_ohlc_data(gateway_ip, symbol, date, cid):
        try:
            print("-------------------------------------------")
            print("Data Recordings started for -", symbol)
            os.chdir(os.getcwd())
            print(os.getcwd())
            subprocess.call(["python3 hrhd_ohlc.py -cid " +
                             str(cid) + " -ip " + gateway_ip + " -p 4002 -s " + symbol + " -d " + date + " -st STK" +
                             " -ds " + " '30 secs' " + " -edt " + " '1 D' "], shell=True)
            print("-------------------------------------------")

        except Exception as ex:
            logger.error(traceback.format_exc())



def main_tick_data():
    cmdLineParser = argparse.ArgumentParser("Vuk History Data Bot :")
    cmdLineParser.add_argument("-ip", "--ip", action="store", type=str,
                               dest="ip", default=hrhdObj.get_value("common","gateway_ip"), help="The IP to get IB Gateway connection")
    cmdLineParser.add_argument("-d", "--date", action="store", type=str,
                               dest="date", default=hrhdObj.get_value("common","hrhd_date"),
                               help="Date (yyyymmdd) For eg: 20190131")
    cmdLineParser.add_argument("-cid", "--cid", action="store", type=int,
                               dest="cid", default=random.randint(1,10), help="Unique client id do request")
    cmdLineParser.add_argument("-list", "--list", action="store", type=str,
                               dest="list", default='nifty50', help="nifty list 50 or 200")
    args = cmdLineParser.parse_args()
    hrhd_obj = Hrhd()
    logger.info("**HRHD Worker Initiated")
    if args.list is not None:
        if args.list == "nifty50":
            nse_dict_list = hrhd_obj.nifty_list('nifty50_list')
        elif args.list == "nifty200":
            nse_dict_list = hrhd_obj.nifty_list('nifty200_list')
        else:
            nse_dict_list = hrhd_obj.nifty_list('nifty_custom_list')
    else:
        nse_dict_list = hrhd_obj.nifty_list('nifty_custom_list')
    logger.info("Read Nifty instruments")
    i = 1
    # print(len(args.))
    if args.ip is not None:
        gateway_ip = args.ip
        hdate = args.date
        cid = args.cid
    else:
        gateway_ip = hrhdObj.get_value('common', 'gateway_ip')
        hdate = hrhdObj.get_value('common', 'hrhd_date')
        cid = 1
    for ins in nse_dict_list:
        logger.info(str(i)+" Of Nifty list "+"("+ins['Symbol']+")")
        hrhd_obj.worker_as_process_ohlc_data(gateway_ip, hrhd_obj.get_ib_symbol_from_map(ins['Symbol']), hdate, random.randint(1,3))
        i = i+1
    fbuObj = FireBaseUtils()
    tick_bank_path = hrhdObj.get_with_base_path('common', 'tick_bank') + "/" + str(hdate) + "/"
    fbuObj.upload_all_file_in_dir_to_firebaseStorage(tick_bank_path, str(hdate))

if __name__ == '__main__':
    import sys
    main_tick_data()






