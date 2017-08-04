from pymongo import MongoClient


class MongodbLoader:
    def __init__(self, namespace='default_namespace', uri='mongodb://127.0.0.1:27017/', db='verint_db',
                 collection='default_collection'):
        """
        :param namespace: a namespace to avoid mixing data from different parties
        :param uri: MongoDB URI
        :param db: database name for MongoDB
        :param collection: collection name for MongoDB
        """
        _client = MongoClient(uri)
        _db = _client["%s_%s" % (namespace, db)]
        self.collection = _db[collection]

    def __call__(self, record):
        self.collection.insert(record)


class StdoutLoader:
    def __call__(self, record):
        print(record)
