class PlaceboTransformer:
    def __call__(self, record):
        return record


class CsvToDictTransformer:
    def __init__(self, headers):
        self.headers = headers

    def __call__(self, record):
        return {k: v for k, v in zip(self.headers, record)}
