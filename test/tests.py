from unittest.mock import patch
from etl import pipe
from etl.extractors import EchoExtractor, HttpJSONExtractor
from etl.loaders import StdoutLoader
from etl.transformers import PlaceboTransformer


class TestExtractors:
    def test_extractor(self):
        data = [{"foo": "bar"}]
        instream = pipe.instream(extractor=EchoExtractor,
                                 extractor_config={'data': [{'foo': 'bar'}, {'lorem': 'ipsum'}, {45: 193}]})
        for i, o in zip(data, instream):
            assert i == o

    def test_extractor_http_json(self):
        data = [{"foo": "bar"}]
        with patch('requests.request') as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = data
            instream = pipe.instream(extractor=HttpJSONExtractor, extractor_config={'url': 'http://blah.com'})
            for i, o in zip(data, instream):
                assert i == o

    def test_transformer(self):
        data = [{"foo": "bar"}, {"lorem": "ipsum"}]
        midstream = pipe.midstream(PlaceboTransformer)
        for record in data:
            transformed_record = midstream.send(record)
            assert record == transformed_record

    def test_transformer_custom_transformer(self):
        data = [113, 21]

        class IncrementTransformer:
            def __init__(self, by):
                self.by = by

            def __call__(self, record):
                return record + self.by

        midstream = pipe.midstream(IncrementTransformer, {"by": 4})
        for record in data:
            transformed_record = midstream.send(record)
            assert record + 4 == transformed_record

    def test_loader(self):
        data = [{"foo": "bar"}]
        outstream = pipe.outstream(StdoutLoader)
        for record in data:
            outstream.send(record)

    def test_pipeline(self):
        data = [2, 4, 6]
        with patch('requests.request') as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = data

            class IncrementTransformer:
                def __init__(self, by):
                    self.by = by

                def __call__(self, record):
                    return record + self.by

            extract_stream = pipe.instream(extractor=HttpJSONExtractor, extractor_config={'url': "http://blah.com"})

            transformer_stream1 = pipe.midstream(transformer=IncrementTransformer, transformer_config={'by': 3})
            transformer_stream2 = pipe.midstream(transformer=IncrementTransformer, transformer_config={'by': 1})

            loader_stream = pipe.outstream(StdoutLoader)
            pipe.flow(extract_stream, loader_stream, transformer_stream1, transformer_stream2)

            """
                Another way of running apipeline
            """
            # while True:
            #     try:
            #         loader_stream.send(transformer_stream1.send(transformer_stream2.send(next(extract_stream))))
            #     except StopIteration:
            #         break
