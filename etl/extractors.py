import requests


class EchoExtractor:
    def __init__(self, data):
        self.data = data

    def __call__(self):
        return self.data


class HttpExtractor:
    def __init__(self, url, headers=None, method='GET', params=None, data=None, timeout=4):
        self.url = url
        self.headers = headers
        self.method = method
        self.params = params
        self.data = data
        self.timeout = timeout

    def request(self):
        try:
            response = requests.request(self.method, self.url, headers=self.headers, params=self.params, data=self.data,
                                        timeout=self.timeout)
        except requests.exceptions.ConnectionError as e:
            raise Exception("While calling url - %s " % str(e))
        if 199 < response.status_code < 300:
            return response
        else:
            raise Exception(
                'End-point call unsuccessful [HTTPCODE %s] [CONTENT %s]' % (response.status_code, response.body[:100]))


class HttpJSONExtractor(HttpExtractor):
    def __call__(self):
        response = self.request()
        record_set = response.json()
        # TODO input schema to extract list of records
        if type(record_set) == dict:
            record_set = [record_set]
        return record_set
