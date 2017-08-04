# ETL Pipeline

ETL is a pipeline why should it be implemented otherwise. Here's a simple Python 3 utility library that lets you pay attention to extraction, transformation and loading business logic and handles pipelining the processes on demand.

All you need is to write callable classes for extractor, transformer and loader and configure you ETL pipeline passing them as input.

For sample usage go through tests, its fairly intuitive.

NOTE: There are default extractor and loader classes implemented, where one of the loaders is MongoDb loader. You may choose to ignore installing pymongo package if you dont plan to use MongoDB loader.

Will extend readme soon with usage and recipes!