import csv

from pymongo import MongoClient


class MongodbLoader:
    def __init__(self, namespace='default_namespace', uri='mongodb://127.0.0.1:27017/', db='etl_pipeline_db',
                 collection='default_collection'):
        """
        MongoDB loader for ETL pipeline. Loads the input stream data into MongoDB.

        :param namespace: a namespace to avoid mixing data of different parties
        :param uri: MongoDB URI
        :param db: database name for MongoDB
        :param collection: collection name for MongoDB
        """
        _client = MongoClient(uri)
        _db = _client["%s_%s" % (namespace, db)]
        self.collection = _db[collection]

    def __call__(self, record):
        self.collection.insert(record)


class CSVLoader:
    def __init__(self, headers, filepath):
        """
        CSV loader for ETL pipeline. Loads the input stream data into a csv file.

        :param headers: list of headers
        :param filepath: absolute path to csv file
        """
        self.headers = headers
        self.writer = csv.writer(open(filepath, 'w'))
        self.writer.writerow(headers)

    def __call__(self, record):
        row = []
        for header in self.headers:
            row.append(record.get(header, ""))
        self.writer.writerow(row)


class StdoutLoader:
    def __call__(self, record):
        print(record)
