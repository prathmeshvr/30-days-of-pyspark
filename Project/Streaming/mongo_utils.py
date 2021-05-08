import pymongo
# from pymongo import MongoClient
from bson.objectid import ObjectId
class mongo_utils():
    @staticmethod
    def insert_data(data_dict:dict):
        conn = pymongo.MongoClient('mongodb+srv://cdac:cdac@cluster0.fa505.mongodb.net/cdac?retryWrites=true&w=majority', 27017)
        db = conn.DB
        db.logs.insert(data_dict)
        conn.close()

    @staticmethod
    def query_last_row(uid:str)->dict:
        conn = pymongo.MongoClient('mongodb+srv://cdac:cdac@cluster0.fa505.mongodb.net/cdac?retryWrites=true&w=majority', 27017)
        db=conn.mydb
        cur=db.myset.find({'uid':uid}).sort('timestamp',pymongo.DESCENDING).limit(1)
        conn.close()
        # e.g.: {'_id': ObjectId('5b45c25f8650f8e6e4b74245'), 'uid': 'test', 'timestamp': '2018-07-11 16:39:59',
        # 'steps': '103','heart_rate':50}
        return cur[0]
    
    @staticmethod
    def update_data(data_dict:dict):
        conn = pymongo.MongoClient('mongodb+srv://cdac:cdac@cluster0.fa505.mongodb.net/cdac?retryWrites=true&w=majority', 27017)
        db = conn.DB
        parameters=[
        {
            "name": "Voltage",
            "value": data_dict['Voltage']
        }, 
        {
            "name": "Current",
            "value": data_dict['Current']
        }, 
        {
            "name": "Power Active",
            "value": data_dict['Power Active']
        }, 
        {
            "name": "Power Apparent",
            "value": data_dict['Power Apparent']
        }, 
        {
            "name": "Power Reactive",
            "value": data_dict['Power Reactive']
        }, 
        {
            "name": "Power Factor",
            "value": data_dict['Power Factor']
        }, 
        {
            "name": "Frequency",
            "value": data_dict['Frequency']
        }, 
        {
            "name": "Imported Energy Active",
            "value": data_dict['Imported Energy Active']
        }, 
        {
            "name": "Exported Energy Active",
            "value": data_dict['Exported Energy Active']
        }, 
        {
            "name": "Imported Energy Reactive",
            "value": data_dict['Imported Energy Reactive']
        }, 
        {
            "name": "Exported Energy Reactive",
            "value": data_dict['Exported Energy Reactive']
        }, 
        {
            "name": "Total Energy Active",
            "value": data_dict['Total Energy Active']
        }, 
        {
            "name": "Total Energy Reactive",
            "value": data_dict['Total Energy Reactive']
        }]

        filter = {'_id':ObjectId("604faa51d9cd50f02ed85894")}
        

        newvalues = { "$set": { 'parameters': parameters } }
        

        # collection.update_one(filter, newvalues) 
        db.assets_collection.update_one(filter, newvalues) 
        conn.close()

