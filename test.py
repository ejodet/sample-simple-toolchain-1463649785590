'''
Created on May 18, 2016

@author: ejd
'''
import logging
import time
import mongoHelpers
import testOTC_ES


def sleepFor1Minute():
    logging.info("Sleeping for 1 minute before retrying....")
    time.sleep(60)
    logging.info("--> Done")
    
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
    jobStartTime= time.time()
    logging.info("Starting test script")
    try:
        logging.info("Trying to connect to Mongo")
        mongoHelpers.connectToMongo()
        logging.info("Starting ES data collection")
        testOTC_ES.main()
        
    except Exception,e:
        logging.info(str(e))
        #mailNotifier.notifyFailure("testOTC", e)
        raise
    
    jobElapsed= time.time() - jobStartTime
    logging.info("test script completed in " + str(jobElapsed) + " ms")
    

if __name__ == "__main__":
    main()