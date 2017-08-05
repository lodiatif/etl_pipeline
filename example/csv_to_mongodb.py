"""
In this example a list of user records is fetched from a local CSV file and loaded into MongoDB collection.
"""

from etl import pipe
from etl.extractors import CSVExtractor
from etl.transformers import CsvToDictTransformer
from etl.loaders import MongodbLoader
import os

# Initialise instream that fetches data from local CSV file.
#
# instream by itself doesnt fetch data, it needs an extractor to do that. Extractors are callable classes that have
# the logic of reading data from source and handing it over to instream in the form of an iterator.
#
# Our source data is CSV list of users from a local CSV file.
# etl-pipeline has a default extractor for reading CSV files.
# To initialise instream, we pass CSVExtractor and provide parameters needed to initialize it.
source_filepath = "%s/csv_to_mongo_source.csv" % os.path.dirname(__file__)
instream = pipe.instream(extractor=CSVExtractor, extractor_config={'csv_file_path': source_filepath})

# Initialise mid-stream that transforms CSV record to a dict.
#
# midstream by itself doesnt transform data, it needs a transformer to do that. Transformers are callable classes that
# have the logic of transforming incoming data and handing it over to midstream.
#
# etl-pipeline has a default transformer for transforming CSV record to a dict.
# To initialise midstream, we pass CsvToDictTransformer and provide parameters needed to initialize it.

headers = ['id', 'first_name', 'last_name', 'email', 'gender', 'ip_address']
midstream = pipe.midstream(transformer=CsvToDictTransformer, transformer_config={'headers': headers})

# Initialise outstream to load data coming from instream to a MongoDB collection.
#
# Just like instream, outstream doesn't load data by itself, it need a loader to do that. Loaders are callable classes
# that have the logic of loading data into storage, outstream provides loaders with data, one record at a time.
#
# We will load the incoming data to a MongoDB collection.
# etl-pipeline has a default loader for loading data in MongoDB.
# To initialise outstream, we pass MongodbLoader class and provide parameters needed to initialize it.

outstream = pipe.outstream(loader=MongodbLoader)

# Eventually we run the ETL pipeline like so..
pipe.flow(instream, outstream, midstream)

# The data should be in MongoDB by now.
