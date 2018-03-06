import pymongo
import datetime
import time
from bson.objectid import ObjectId
from Logger import Logger
import re
import configparser
import logging
import os
from DataAccess import DataAccess

class requestAccess(DataAccess):

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
        totalCount = self.db['Requests'].find({'status': {"$in": ['modified','created','processing','finished','failed']}}).count()

        result = {
            "totalCount":totalCount,
            "data": self.db['Requests'].find({'status': {"$in": ['modified','created','processing','finished','failed']}}).sort("createDate", pymongo.DESCENDING).skip(skips).limit(pageSize)
        }
        return result

    def get_created_requests(self):
        return self.db['Requests'].find({'status': {"$in": ['created']}})

    def get_created_request(self):
        return self.db['Requests'].find_one({'status': 'created'})

    def get_modified_requests(self):
        return self.db['Requests'].find({'status': {"$in": ['modified']}})

    def get_processing_requests(self):
        return self.db['Requests'].find({'status': {"$in": ['processing']}})

    def get_failed_request(self):
        return self.db['Requests'].find_one({'status': 'failed'})

    def get_failed_requests(self):
        return self.db['Requests'].find({'status': {"$in": ['failed']}})

    def get_removed_requests(self):
        return self.db['Requests'].find({'status': {"$in": ['removed']}})

    def processing_requests(self, id, searchKey, totalCount):
        # self.db[self.db['Requests'].find_one({'_id': ObjectId(id)})['requestId']].create_index("requestId", unique=True)
        return self.db['Requests'].update_one({'_id': ObjectId(id), 'searchKeys.key':searchKey}, {'$set': {'status': 'processing', 'searchKeys.$.count':totalCount}})
    
    def processing_request(self, id):
        return self.db['Requests'].update_one({'_id': ObjectId(id)}, {'$set': {'status': 'processing'}})
    
    def finish_requests(self, id):
        return self.db['Requests'].update_one({'_id': ObjectId(id)}, {'$set': {'status': 'finished'}})

    def remove_request(self, id):
        return self.db['Requests'].update_one({'_id': ObjectId(id)}, {'$set': {'status': 'removed'}})

    def failed_request(self, id):
        return self.db['Requests'].update_one({'_id': ObjectId(id)}, {'$set': {'status': 'failed'}})

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