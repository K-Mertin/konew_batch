from DataAccess import DataAccess
from bson.objectid import ObjectId
from operator import attrgetter
import pymongo
import datetime


class relationAccess(DataAccess):
    def insert_relation(self, relation):
        print(relation)
        relation['createDate'] = datetime.datetime.utcnow()
        relation['modifyDate'] = datetime.datetime.utcnow()
        relation['createUser'] = relation['user']
        relation['modifyUser'] = relation['user']
        relation['status'] = 'lived'
        relation.__delitem__('user')

        id = self.db['Relations'].insert(relation)

        ret = self.log_modify(id, 'insert', relation['modifyUser'])

        return ret

    def get_logs(self, id):
        print(id)
        return self.db['RelationLog'].find({'relation._id': ObjectId(id)}).sort("date", pymongo.ASCENDING)

    def delete_relation(self, id, user, ip):
        self.logger.logger.info('delete_relation:' + id )

        self.db['Relations'].update_one({'_id': ObjectId(id)}, {'$set': {'status': 'deleted', 'modifyUser':user}})

        ret = self.log_modify(id, 'deleted', user, ip)

        return ret

    def get_relations(self, queryType, key):
        if queryType == 'reason':
            return self.db['Relations'].find({queryType: key})

        return self.db['Relations'].find({'$or': [{'objects.'+queryType: key}, {'subjects.'+queryType: key}]})

    def get_relation_keyList(self, queryType, key):
        if queryType == 'reason':
            print('/^'+key+'/')
            return self.db['Relations'].aggregate([{'$project': {'reason': 1}}, {'$group': {'_id': '$reason'}}, {'$match': {'_id': {'$regex': '^'+key}}}])

        return self.db['Relations'].aggregate([{'$project': {"combined":  {'$setUnion': ["$subjects."+queryType, "$objects."+queryType]}}}, {"$unwind": "$combined"}, {"$group": {"_id": "$combined"}}, {"$match": {"_id": {'$regex': '^'+key}}}])

    def update_relation(self, relation, ip):
        id = relation['_id']
        relation['modifyDate'] = datetime.datetime.utcnow()
        relation['modifyUser'] = relation['user']
        relation.__delitem__('user')
        relation.__delitem__('_id')

        self.db['Relations'].update({'_id': ObjectId(id)}, relation)

        ret = self.log_modify(id, 'update', relation['modifyUser'], ip)

        return ret

    def download_relations(self):
        return self.db['Relations'].aggregate([{"$unwind": "$subjects"}, {"$unwind": "$objects"}, {'$project': {
            "subjects": 1,
            "reason": 1,
            "objects.name": 1,
            "objects.idNumber": 1,
            "objects.memo": 1,
            "objects.relationType": {
                "$map": {
                    "input": "$objects.relationType",
                    "as": "el",
                    "in": "$$el.itemName"
                }
            },
            "createDate": 1,
            "createUser": 1,
            "modifyDate": 1,
            "modifyUser": 1,
        }}])

    def log_modify(self, id, action, user, ip=''):
        return self.db['RelationLog'].insert({
            'action': action,
            'relation': self.db['Relations'].find_one({'_id': ObjectId(id)}),
            'date': datetime.datetime.utcnow(),
            'user': user,
            'ip': ip
        })
    
    def get_connections(self, idNumber):
        return self.db['Relations'].aggregate([
            {'$match': { '$or': [{ 'subjects.idNumber': idNumber }, { 'objects.idNumber': idNumber }] }},
            {'$project':{'reason':1, 'subjects':1,'objects':1}},
            {'$unwind':'$objects'},
            {'$unwind':'$subjects'}])

    def check_duplicate(self, target , id):
        return self.db['Relations'].find({target+'.idNumber': id}).count()

if __name__ == "__main__":
    db = relationAccess()
