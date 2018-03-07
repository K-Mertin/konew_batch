from DataAccess import DataAccess
from bson.objectid import ObjectId
from operator import attrgetter
import pymongo
import datetime


class relationAccess(DataAccess):
    collectionName = 'Relations'

    def insert_relation(self, relation):
        print(relation)
        relation['createDate'] = datetime.datetime.utcnow()
        relation['modifyDate'] = datetime.datetime.utcnow()
        relation['createUser'] = relation['user']
        relation['modifyUser'] = relation['user']
        relation['status'] = 'lived'
        relation.__delitem__('user')

        id = self.db[self.collectionName].insert(relation)

        ret = self.log_modify(id, 'insert', relation['modifyUser'])

        return ret

    def get_relations(self, queryType, key):
        if queryType == 'reason':
            return self.db[self.collectionName].find({queryType: key})

        return self.db[self.collectionName].find({'$or': [{'objects.'+queryType: key}, {'subjects.'+queryType: key}]})

    def update_relation(self, relation, ip):
        id = relation['_id']
        relation['modifyDate'] = datetime.datetime.utcnow()
        relation['modifyUser'] = relation['user']
        relation.__delitem__('user')
        relation.__delitem__('_id')

        self.db[self.collectionName].update({'_id': ObjectId(id)}, relation)

        ret = self.log_modify(id, 'update', relation['modifyUser'], ip)

        return ret

    def log_modify(self, id, action, user, ip=''):
        return self.db['RelationLog'].insert({
            'action': action,
            'relation': self.db[self.collectionName].find_one({'_id': ObjectId(id)}),
            'date': datetime.datetime.utcnow(),
            'user': user,
            'ip': ip
        })

    def get_relation(self, idNumber):
        return self.db[self.collectionName].find_one({'subjects.idNumber': idNumber})


if __name__ == "__main__":
    db = relationAccess()
