import time, os
os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()
import os.path
import datetime
import traceback
import os
import csv
import sys
sys.path.append(os.getcwd()[:os.getcwd().find("HRHD_Worker")+len("HRHD_Worker")])
from kafka import KafkaProducer
import json
from json import dumps
from time import sleep
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         linger_ms=10,
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
class HrhdOhlcStreamer(object):
    strdate=None
    strsymbol=None
    def __init__(self, strdate, strsymbol ):
        self.strdate = strdate
        self.strsymbol = strsymbol
        self.stream_ohlc()

    def check_csv_exist(self):
        pass

    def stream_ohlc(self):
        f = open("/Users/msivaanand/siva_projects/HRHD_Worker/tickbank/"+self.strdate+"/"+self.strsymbol+"_STK.csv")
        # f = open("/Users/msivaanand/siva_projects/HRHD_Worker/tickbank/"+self.strdate+"/"+self.strsymbol+"_ticks.csv")
        csv_f = csv.reader(f)
        for index,row in enumerate(csv_f):
            if index > 0:

                try:
                    data = {"topicId": self.strsymbol, "Price": float(row[1]), "Timestamp": str(int(row[0]))}
                    # producer.send(str(self.strsymbol), value=data).get(timeout=30)
                    print(data)
                    ack = producer.send(str(self.strsymbol), value=data).get(timeout=5)
                    sleep(0.03)
                    producer.flush()
                except Exception as ex:
                    print(ex)

if __name__ == "__main__":
    obj = HrhdOhlcStreamer('20200424','CIPLA')
