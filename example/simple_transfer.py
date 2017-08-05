"""
In this example a list of user records is fetched from JSONPlaceholder fake REST API and loaded into CSV file.
This example doesnt perform any transformation to data so we wont cover Transformers in it.
"""

from etl import pipe
from etl.extractors import HttpJSONExtractor
from etl.loaders import CSVLoader
import os

# Initialise instream that fetches data from API.
#
# instream by itself doesnt fetch data, it needs an extractor to do that. Extractors are callable classes that have
# the logic of reading data from source and handing it over to instream in the form of an iterator.
#
# Our source data is JSON list of users from https://jsonplaceholder.typicode.com/users
# etl-pipeline has a default extractor for reading JSON from REST API - HttpJSONExtractor
# To initialise instream, we pass HttpJSONExtractor and provide parameters needed to initialize it.

data_source_api = 'https://jsonplaceholder.typicode.com/users'
instream = pipe.instream(extractor=HttpJSONExtractor, extractor_config={'url': data_source_api})

# Initialise outstream to load data coming from instream to CSV file.
#
# Just like instream, outstream doesn't load data by itself, it need a loader to do that. Loaders are callable classes
# that have the logic of loading data into storage, outstream provides loaders with data, one record at a time.
#
# We will load the incoming data to a CSV file.
# etl-pipeline has a default loader for loading data in CSV file.
# To initialise outstream, we pass CSVLoader class and provide parameters needed to initialize it.

filepath = "%s/'simple_transfer.csv'" % os.path.dirname(__file__)
headers = ['id', 'name', 'username', 'email', 'address', 'phone', 'website', 'company']
outstream = pipe.outstream(loader=CSVLoader, loader_config={'filepath': filepath, 'headers': headers})

# Eventually we run the ETL pipeline like so..
pipe.flow(instream, outstream)

# The data should be in CSV file by now.
