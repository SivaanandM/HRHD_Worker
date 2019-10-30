import collections
import time
import os.path
import argparse
import datetime
import traceback
from random import randint
import csv
import subprocess
import os
import sys
sys.path.append(os.getcwd()[:os.getcwd().find("HRHD_Worker")+len("HRHD_Worker")])

import random
from ibapi import *
from ibapi.utils import *
from ibapi.common import *
from ibapi.contract import *
from ibapi import wrapper
from ibapi.client import EClient
from ibapi.utils import iswrapper
from ibapi.scanner import ScanData
from src.hrhd_worker.hrhd_object import HRHDObjects as hrhdObj
from kafka import KafkaProducer
import json
from json import dumps

producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

def clear():
    if os.name in ('nt', 'dos'):
        subprocess.call("cls")
    elif os.name in ('linux', 'osx', 'posix'):
        subprocess.call("clear")
    else:
        print("\n") * 120


def printWhenExecuting(fn):
    def fn2(self):
        print("   doing", fn.__name__)
        fn(self)
        print("   done w/", fn.__name__)

    return fn2


def printinstance(self, inst: Object):
    attrs = vars(inst)
    print(', '.join("%s: %s" % item for item in attrs.items()))


class Activity(Object):
    def __init__(self, reqMsgId, ansMsgId, ansEndMsgId, reqId):
        self.reqMsdId = reqMsgId
        self.ansMsgId = ansMsgId
        self.ansEndMsgId = ansEndMsgId
        self.reqId = reqId


class RequestMgr(Object):
    def __init__(self):
        self.requests = []

    def addReq(self, req):
        self.requests.append(req)

    def receivedMsg(self, msg):
        pass


# ! [socket_declare]
class VukClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        # ! [socket_declare]

        # how many times a method is called to see test coverage
        self.clntMeth2callCount = collections.defaultdict(int)
        self.clntMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nReq = collections.defaultdict(int)
        self.setupDetectReqId()

    def countReqId(self, methName, fn):
        def countReqId_(*args, **kwargs):
            self.clntMeth2callCount[methName] += 1
            idx = self.clntMeth2reqIdIdx[methName]
            if idx >= 0:
                sign = -1 if 'cancel' in methName else 1
                self.reqId2nReq[sign * args[idx]] += 1
            return fn(*args, **kwargs)

        return countReqId_

    def setupDetectReqId(self):

        methods = inspect.getmembers(EClient, inspect.isfunction)
        for (methName, meth) in methods:
            if methName != "send_msg":
                # don't screw up the nice automated logging in the send_msg()
                self.clntMeth2callCount[methName] = 0
                # logging.debug("meth %s", name)
                sig = inspect.signature(meth)
                for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                    (paramName, param) = pnameNparam  # @UnusedVariable
                    if paramName == "reqId":
                        self.clntMeth2reqIdIdx[methName] = idx

                setattr(VukClient, methName, self.countReqId(methName, meth))

                # print("TestClient.clntMeth2reqIdIdx", self.clntMeth2reqIdIdx)


# ! [ewrapperimpl]
class VukWrapper(wrapper.EWrapper):
    # ! [ewrapperimpl]
    def __init__(self):
        wrapper.EWrapper.__init__(self)

        self.wrapMeth2callCount = collections.defaultdict(int)
        self.wrapMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nAns = collections.defaultdict(int)
        self.setupDetectWrapperReqId()

    # TODO: see how to factor this out !!

    def countWrapReqId(self, methName, fn):
        def countWrapReqId_(*args, **kwargs):
            self.wrapMeth2callCount[methName] += 1
            idx = self.wrapMeth2reqIdIdx[methName]
            if idx >= 0:
                self.reqId2nAns[args[idx]] += 1
            return fn(*args, **kwargs)

        return countWrapReqId_

    def setupDetectWrapperReqId(self):

        methods = inspect.getmembers(wrapper.EWrapper, inspect.isfunction)
        for (methName, meth) in methods:
            self.wrapMeth2callCount[methName] = 0
            # logging.debug("meth %s", name)
            sig = inspect.signature(meth)
            for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                (paramName, param) = pnameNparam  # @UnusedVariable
                # we want to count the errors as 'error' not 'answer'
                if 'error' not in methName and paramName == "reqId":
                    self.wrapMeth2reqIdIdx[methName] = idx

            setattr(VukWrapper, methName, self.countWrapReqId(methName, meth))


class TickHistoryStreamer(VukWrapper, VukClient):
    HDATE = ""  # "20190131"
    SYMBOL = ""  # "DLF"
    IP = ""
    PORT = 4001
    SECTYPE = ""
    STRIKE = ""
    RIGHT = ""
    EXPIRY = ""
    contract = Contract()

    def __init__(self):
        VukWrapper.__init__(self)
        VukClient.__init__(self, wrapper=self)
        # ! [socket_init]
        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.reqId2nErr = collections.defaultdict(int)
        self.globalCancelOnly = False
        self.simplePlaceOid = None

    def dumpTestCoverageSituation(self):
        for clntMeth in sorted(self.clntMeth2callCount.keys()):
            logging.debug("ClntMeth: %-30s %6d" % (clntMeth,
                                                   self.clntMeth2callCount[clntMeth]))

        for wrapMeth in sorted(self.wrapMeth2callCount.keys()):
            logging.debug("WrapMeth: %-30s %6d" % (wrapMeth,
                                                   self.wrapMeth2callCount[wrapMeth]))

    def dumpReqAnsErrSituation(self):
        logging.debug("%s\t%s\t%s\t%s" % ("ReqId", "#Req", "#Ans", "#Err"))
        for reqId in sorted(self.reqId2nReq.keys()):
            nReq = self.reqId2nReq.get(reqId, 0)
            nAns = self.reqId2nAns.get(reqId, 0)
            nErr = self.reqId2nErr.get(reqId, 0)
            logging.debug("%d\t%d\t%s\t%d" % (reqId, nReq, nAns, nErr))

    @iswrapper
    # ! [historicaltickslast]
    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast,
                            done: bool):
        self.fulldaydata(ticks)

    @staticmethod
    def connect_ib(ib_connection):
        try:
            if not ib_connection.isConnected():
                ib_connection.connect(TWS_IP, TMS_PORT, clientId=IB_CLIENT_ID)
                log.info("serverVersion:%s connectionTime:%s" % (ib_connection.serverVersion(),
                                                                 ib_connection.twsConnectionTime()))
        except Exception as ex:
            log.error(ex)
            log.error(traceback.format_exc())

    def fulldaydata(self, ticks):
        try:
            # tickbank_path = hrhdObj.get_with_base_path('common', 'tick_bank') + "/" + str(self.HDATE) + "/"
            # with open(tickbank_path+self.SYMBOL + "_" + self.SECTYPE + ".csv", 'a', newline='') as csvfile:
            #     filewriter = csv.writer(csvfile, delimiter=',',
            #                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
            if len(ticks) >= 1000:
                i = 0
                for tick in ticks:
                    print(str(time.strftime("%D %H:%M:%S", time.localtime(int(tick.time)))) + "," + str(
                        tick.price) + "," + str(tick.size))
                    data = {"topicId": "HRHD", "Price": tick.price, "Timestamp": str(int(tick.time))}
                    producer.send(str("HRHD"), value=data)
                    i = i + 1
                    if i == 1000:
                        # self.connect_ib(self)
                        self.reqHistoricalTicks(randint(100, 200), self.contract,
                                                str(self.HDATE) + " " + str(
                                                    time.strftime("%H:%M:%S", time.localtime(int(tick.time)))), "",
                                                1000, "TRADES", 1, True, [])
            else:
                for tick in ticks:
                    print(str(time.strftime("%D %H:%M:%S", time.localtime(int(tick.time))))+","+str(tick.price)+","+str(tick.size))
                    data = {"topicId": "HRHD", "Price": tick.price, "time": str(int(tick.time))}
                    producer.send(str("HRHD"), value=data)
                sys.exit()

        except Exception as e:
            logging.error(traceback.format_exc())
            print(traceback.format_exc())
        finally:
            self.dumpTestCoverageSituation()
            self.dumpReqAnsErrSituation()

    @iswrapper
    # ! [currenttime]
    def currentTime(self, time: int):
        super().currentTime(time)
        print("CurrentTime:", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"))
    # ! [currenttime]

    def tick_data_req_parameter(self, args_symbol, args_date, args_ip="127.0.0.1", args_cid=1, args_port=4002, args_sectype='STK', args_expiry=None,
                      args_strike=None, args_right=None):
        try:
            # app = TickHistory()
            self.SYMBOL = args_symbol
            self.HDATE = args_date
            self.IP = args_ip
            self.PORT = args_port
            self.SECTYPE = args_sectype
            print("\n## Started ##")
            print("\nUsing args", args_symbol, args_date, args_ip, args_port, args_sectype, args_expiry, args_strike, args_right)
            self.connect(self.IP, self.PORT, args_cid)
            print("\nIB Gateway Time:%s connectionTime:%s" % (self.serverVersion(),
                                                            self.twsConnectionTime()))
            print("\n~~ Recorded HRHD for "+self.SYMBOL+", DATE :  "+str(self.HDATE))

            self.contract.symbol = self.SYMBOL
            self.contract.currency = "INR"
            self.contract.exchange = "NSE"
            if self.SECTYPE == "STK":
                self.contract.secType = self.SECTYPE
            elif self.SECTYPE == "FUT":
                self.EXPIRY = args_expiry
                self.contract.secType = self.SECTYPE
                self.contract.lastTradeDateOrContractMonth = self.EXPIRY
            elif self.SECTYPE == "OPT":
                self.STRIKE = args_strike
                self.RIGHT = args_right
                self.EXPIRY = args_expiry
                self.contract.secType = self.SECTYPE
                self.contract.lastTradeDateOrContractMonth = self.EXPIRY
                self.contract.strike = self.STRIKE
                self.contract.right = self.RIGHT
            self.reqHistoricalTicks(random.randint(1,100), self.contract,
                                   str(self.HDATE) + " 09:10:00", "", 1000, "TRADES", 1, True, [])
            self.run()

        except Exception:
            logging.error(traceback.format_exc())
            print("\n")
            print("\n~~~~~~~~~~~~~~~~~~~~~~~~ Error ~~~~~~~~~~~~~~~~~~~~~~~")
            print("\nError : " + traceback.format_exc())
            print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        finally:
            self.dumpTestCoverageSituation()
            self.dumpReqAnsErrSituation()
            print("\n## Completed ##")


def main_tick_data():

    cmdLineParser = argparse.ArgumentParser("Vuk History Data Bot :")
    cmdLineParser.add_argument("-cid", "--cid", action="store", type=int,
                               dest="cid", default=random.randint(1,10), help="Unique client id do request")
    cmdLineParser.add_argument("-ip", "--ip", action="store", type=str,
                               dest="ip", default="127.0.0.1", help="The IP to get IB Gateway connection")
    cmdLineParser.add_argument("-p", "--port", action="store", type=int,
                               dest="port", default=4002, help="The TCP port to use For eg: 1122")
    cmdLineParser.add_argument("-s", "--symbol", action="store", type=str,
                               dest="symbol", default="INFY",
                               help="Instrument Symbol For eg: INFY ")
    cmdLineParser.add_argument("-d", "--date", action="store", type=str,
                               dest="date", default="20190131",
                               help="Date (yyyymmdd) For eg: 20190131")
    cmdLineParser.add_argument("-st", "--sectype", action="store", type=str,
                               dest="sectype", default="STK",
                               help="Security Type For eg: 'STK','FUT','OPT'")
    cmdLineParser.add_argument("-e", "--expiry", action="store", type=str,
                               dest="expiry", default="",
                               help="Expiry Date For eg: FUT-201903, OPT-20190315")
    cmdLineParser.add_argument("-sp", "--strike", action="store", type=str,
                               dest="strike", default="",
                               help="Option Strike Price For eg: 11222.50")
    cmdLineParser.add_argument("-r", "--right", action="store", type=str,
                               dest="right", default="",
                               help="Option Rights For eg: C or P")
    args = cmdLineParser.parse_args()
    app = TickHistoryStreamer()
    app.tick_data_req_parameter(
        args_symbol=args.symbol,
        args_date=args.date,
        args_ip=args.ip,
        args_cid=args.cid,
        args_port=args.port,
        args_sectype=args.sectype,
        args_expiry=args.expiry,
        args_strike=args.strike,
        args_right=args.right
    )


if __name__ == "__main__":
    # main_tick_data()
    app = TickHistoryStreamer()
    app.tick_data_req_parameter(args_symbol="TCS", args_date="20191030")
