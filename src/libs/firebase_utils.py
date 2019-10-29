import sys
import os
sys.path.append(os.getcwd()[:os.getcwd().find("HRHD_Worker")+len("HRHD_Worker")])
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage

from src.hrhd_worker.hrhd_object import HRHDObjects as hrdObj

from src.loghandler import log
logger = log.setup_custom_logger('hrhd')



class FireBaseUtils:

    cred = None
    bucket = None


    def __init__(self):
        try:
            logger.info("Initiating connection to fire base")
            FireBaseUtils.cred = credentials.Certificate(hrdObj.get_with_base_path("firebase", "firebase_key"))
            firebase_admin.initialize_app(FireBaseUtils.cred, {
                'storageBucket': hrdObj.get_value('firebase', 'firebase_app_name')
            })
            logger.info("Connected to firebase bucket")
            FireBaseUtils.bucket = storage.bucket()
        except Exception as ex:
            logger.error(ex)

    def upload_all_file_in_dir_to_firebaseStorage(self, dir_path, date):
        try:
            for r, d, f in os.walk(dir_path):
                for file in f:
                    if '.csv' in file:
                        print("Uploading file @"+os.path.join(r, file))
                        blob = FireBaseUtils.bucket.blob(date+"/"+file)
                        blob.upload_from_filename(os.path.join(r, file))
                        logger.info('File {} uploaded to {}.'.format(
                            os.path.join(r, file),
                            date+"/"+file))
        except Exception as ex:
            logger.error(ex)


if __name__ == '__main__':
    fbuObj = FireBaseUtils()
    fbuObj.upload_all_file_in_dir_to_firebaseStorage("/Users/sivaamur/Vuk-ai/GitRepos/TickStream/tickbank/20190830/",
                                                     "20190830")