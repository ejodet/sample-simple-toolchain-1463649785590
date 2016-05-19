import os  
import json  
import pymongo
import logging

def connectToMongo():
    #get the VCAP_SERVICES env. variable
    logging.info("Connecting to Mongo")
    services = os.getenv('VCAP_SERVICES') 
    #convert to json:
    services_json = json.loads(services)  
    #get the mongoDB url:
    mongodb_url = services_json['mongodb-2.4'][0]['credentials']['url']
    #connect:
    client = pymongo.MongoClient(mongodb_url)  
    #get the default database:
    db = client.get_default_database() 
    logging.info ('connected to mongodb!')  
    return db