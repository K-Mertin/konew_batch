import pymongo
import datetime
import time
from pymongo import MongoClient
from bson.objectid import ObjectId
from Logger import Logger
import re
import configparser
import logging
import os

class DataAccess:

    def __init__(self):
        self.logger = Logger('DataAccess')
        self.Setting()
        self.client = MongoClient(self.ipAddress, username=self.user , password=self.password, authSource=self.dbName )
        self.db = self.client[self.dbName]
        self.logger.logger.info( 'Finish initializing DataAccess')
       
    def Setting(self):
        self.config = configparser.ConfigParser()
        with open('Config.ini') as file:
            self.config.readfp(file)

        # self.logPath = self.config.get('Options','Log_Path')
        self.ipAddress = self.config.get('Mongo','ipAddress')
        self.dbName = self.config.get('Mongo','dbName')
        self.user = self.config.get('Mongo','user')
        self.password = self.config.get('Mongo','password')
        
        # formatter = logging.Formatter('[%(name)-12s %(levelname)-8s] %(asctime)s - %(message)s')
        # self.logger=logging.getLogger(__class__.__name__)
        # self.logger.setLevel(logging.DEBUG)
        
        # if not os.path.isdir(self.logPath):
        #     os.mkdir(self.logPath)

        # fileHandler = logging.FileHandler(self.logPath+__class__.__name__+'_log.txt')
        # fileHandler.setLevel(logging.INFO)
        # fileHandler.setFormatter(formatter)

        # streamHandler = logging.StreamHandler()
        # streamHandler.setLevel(logging.DEBUG)
        # streamHandler.setFormatter(formatter)

        # self.logger.addHandler(fileHandler)
        # self.logger.addHandler(streamHandler)

        # self.logger.logger.info('Finish DataAccess Setting')

    def add_request(self, request):
        request['status'] = 'created'
        request['createDate'] = datetime.datetime.now()
        request['requestId'] = str(datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))+'-'+request['searchKeys'][0]
        request['searchKeys'] = list(map(lambda x:{"key":x, "count":0}, request['searchKeys']))
        return self.db['Requests'].insert(request)

    def change_request_reference(self, id, refKey):
        return self.db['Requests'].update_one({'_id': ObjectId(id)}, {'$set': {'referenceKeys': refKey, 'status': 'modified'}})
    
    def update_document_reference(self, collection, id, referenceKeys ):
        return self.db[collection].update_one({'_id': ObjectId(id)}, {'$set': {'referenceKeys': referenceKeys}})

    def update_document_searchKeys(self, collection, id, searchKeys ):
        return self.db[collection].update_one({'_id': ObjectId(id)}, {'$set': {'searchKeys': searchKeys}})

    def get_allPaged_requests(self, pageSize=10, pageNum=1):
        skips = pageSize * (pageNum - 1)
        totalCount = self.db['Requests'].find({'status': {"$in": ['modified','created','processing','finished']}}).count()

        result = {
            "totalCount":totalCount,
            "data": self.db['Requests'].find({'status': {"$in": ['modified','created','processing','finished']}}).sort("createDate", pymongo.DESCENDING).skip(skips).limit(pageSize)
        }
        return result

    def get_created_requests(self):
        return self.db['Requests'].find({'status': {"$in": ['created']}})

    def get_modified_requests(self):
        return self.db['Requests'].find({'status': {"$in": ['modified']}})

    def get_processing_requests(self):
        return self.db['Requests'].find({'status': {"$in": ['processing']}})

    def get_removed_requests(self):
        return self.db['Requests'].find({'status': {"$in": ['removed']}})

    def processing_requests(self, id, searchKey, totalCount):
        # self.db[self.db['Requests'].find_one({'_id': ObjectId(id)})['requestId']].create_index("requestId", unique=True)
        return self.db['Requests'].update_one({'_id': ObjectId(id), 'searchKeys.key':searchKey}, {'$set': {'status': 'processing', 'searchKeys.$.count':totalCount}})

    def finish_requests(self, id):
        return self.db['Requests'].update_one({'_id': ObjectId(id)}, {'$set': {'status': 'finished'}})

    def remove_request(self, id):
        return self.db['Requests'].update_one({'_id': ObjectId(id)}, {'$set': {'status': 'removed'}})

    def insert_documents(self, collection, documents):
        return self.db[collection].insert_many(documents)

    def get_allPaged_documents(self, collection='1514966746.2558856', pageSize=10, pageNum=1, sortBy="keys", filters=[]):
        skips = pageSize * (pageNum - 1)
        # print(filters)
        filters = list(map(lambda x : [{"$or":[{'searchKeys':x},{'referenceKeys':x},{'tags':x}]}],filters ))
        filters = sum(filters,[])

        aggregateList = []
        aggregateList.append(
            {
                        "$project":{
                            "searchKeys":1,
                            "referenceKeys":1,
                            "tags":1,
                            "title": 1,
                            "content":1,
                            "source":1,
                            "date":1,
                            "rkLength":{"$size":"$referenceKeys"},
                            "skLength":{"$size":"$searchKeys"},
                    }}
        )
        if len(filters) >0:
            aggregateList.append( {  "$match":{"$and":filters}} )

        if sortBy == "keys":
            aggregateList.append( { "$sort": {"skLength":-1,"rkLength":-1, "date": -1}})
        else:
            aggregateList.append( { "$sort": {"skLength":-1,"date": -1,"rkLength":-1}})

        # print(aggregateList)
        aggregateList.append( { "$skip": skips})

        aggregateList.append( { "$limit": pageSize })

        return self.db[collection].aggregate(aggregateList)
    
    def get_documents_count(self, collection, filters=[]):

        if len(filters) >0:
            filters = list(map(lambda x : [{"$or":[{'searchKeys':x},{'referenceKeys':x},{'tags':x}]}],filters ))
            filters = sum(filters,[])
            filters = {"$and":filters}
            return self.db[collection].find(filters).count()
        else:
            return self.db[collection].find().count()

    def remove_all_documents(self, collection):
        return self.db[collection].drop() 

if __name__ == "__main__":
    # db = DataAccess()
    # content =db.db['20180116173253695722-邱彪&蔡麗芬'].find()[1]['content'].replace('\n','')
    
    # key= '蔡麗芬'
    # pattern =''

    # for index in range(len(key)):
    #     if index == len(key)-1:
    #         pattern = pattern+key[index]
    #     else:
    #         pattern = pattern+key[index] + '\s*'

    # print(re.search(pattern,content))
    # print(content)
    # name = input('請輸入檔名：')
    # file = open('a.txt', 'r',encoding='ANSI')
    # content = file.read()
    # print(content)
# ?\    file.close()

    str1 = '�W�O�_�a��k�|�D�ƧP�M'

    print(str1.encode('ANSI'))
    # print(.replace('\n',''))

    # request = {
    #     "searchKeys": ["",""],
    #     "referenceKeys": ["臺中"]
    # }
    # documents = [
    #     {
    #         "searchKeys":["康業資本"],
    #         "referenceKeys":["RK1"],
    #         "tags":["tagA","tagB"],
    #         "title": "title A",
    #         "content":"content A"
    #     },
    #     {
    #          "searchKeys":["康業資本"],
    #         "referenceKeys":["RK2"],
    #         "tags":["tagA","tagC"],
    #         "title": "title B",
    #         "content":"content B"
    #     }
    # ]
    # # refKey = ['A']
    # refKey.append('B')

    # remove_request(db,id)
    # results=db.get_all_documents()
    # db.remove_all_documents('20180110075949138853-康業資本')
    # results = db.db.get
    # db.change_reference('5a4ca418f6fadc82283bba6a',['臺中','基隆'])
    # print(db.get_modified_requests().count())
    # print(db.db['Requests'].update_one({'_id': ObjectId("5a4ca418f6fadc82283bba6a")},{'$set': {'requestId':'1514966746.2558856'}}))
    # results = db.get_all_documents('1514966746.2558856',5,2),z


    # db.change_reference('5a4ca418f6fadc82283bba6a',['嚴永誠','基隆','魏樹達','台北','臺北'])
    # # print(db.get_documents_count('1514966746.2558856'))
    # for re in results:
    #     print(re['referenceKeys'])


    # for result in results :
    #     print(result['requestId'])
        # processing_requests(db,result['_id'])
        # insert_documents(db,str(result['requestId'])+result['searchKey'][0],documents)
    # result=add_request(db,request)
