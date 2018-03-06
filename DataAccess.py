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
        try:
            self.Setting()
            self.client = MongoClient(self.ipAddress, username=self.user , password=self.password, authSource=self.dbName )
            self.db = self.client[self.dbName]
        except Exception as e:
            self.logger.logger.error(e)
            raise(e)
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

if __name__ == "__main__":
    db = DataAccess()
    content =db.db['Requests'].find_one({'status': 'finished'})
    if content:
        print(content['requestId'])
    else:
        print('none')

    # title =db.db['20180116194535488260-邱彪&蔡麗芬'].find()[1]['title'].replace('\n','')
    # # key= '蔡麗芬'
    # # pattern =''

    # # for index in range(len(key)):
    # #     if index == len(key)-1:
    # #         pattern = pattern+key[index]
    # #     else:
    # #         pattern = pattern+key[index] + '\s*'

    # # print(re.search(pattern,content))
    # # print(content)
    # # name = input('請輸入檔名：')

    # file = open('a.txt', 'r', encoding='ANSI')
    # test = file.read()
    # print(test)
    # # file.write(content.encode())
    # # content = file.read()
    # # print(content)
    # # file.close()
    # print(title)

    # print(content[0:22])
    # # print(.replace('\n',''))

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
