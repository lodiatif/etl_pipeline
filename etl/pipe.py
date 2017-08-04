from functools import wraps


def prime(stream_f):
    @wraps(stream_f)
    def wrapper(*args, **kwargs):
        stream = stream_f(*args, **kwargs)
        next(stream)
        return stream

    return wrapper


def instream(extractor, extractor_config={}):
    """
    This is the inlet of ETL pipeline. It's channel to send a stream of records to the mid or out-stream. The function
    is a generator that yields source data; essentially it's a generator wrapper around source data iterator, so by
    itself the in-stream does not connect to any other streams directly, the pipelining is managed by the client.

    :param extractor: a callable class that returns an iterable on source data
    :param extractor_config: dictionary of parameters required to initialise instance of extractor class
    """
    extractor_instance = extractor(**extractor_config)
    for record in extractor_instance():
        yield record


@prime
def midstream(transformer, transformer_config={}):
    """
    This is transformation channel in ETL pipeline, one pipeline can have multiple mid-streams. It receives stream of
    records from in-stream or previous transformation channels, apply its own transformation and forward the
    transformed record to another mid-stream or out-stream. The function is a coroutine that receives data stream,
    facilitates record transformation and yields the transformed record. Mid-stream doesnt connect to any other streams
    directly, the pipelining is managed by the client.

    :param transformer: a callable class that transforms input record and returns it
    :param transformer_config: dictionary of parameters required to initialise instance of transformer class
    """
    transformer_instance = transformer(**transformer_config)
    transformed_record = None
    while True:
        try:
            record = yield transformed_record
            transformed_record = transformer_instance(record)
        except Exception as e:
            raise


@prime
def outstream(loader, loader_config={}):
    """
    This is the outlet of pipeline. A channel to receive a stream of records from in-stream or mid-stream, and load it
    to a storage. The function is a coroutine that receives potentially transformed data stream that can be loaded.
    Out-stream doesnt connect to any other streams directly, the pipelining is managed by the client.

    :param loader: a callable class that loads input record to a suitable storage
    :param loader_config: dictionary of parameters required to initialise instance of loader class
    """
    loader_instance = loader(**loader_config)
    while True:
        record = yield
        loader_instance(record)


def flow(instream, outstream, *middle_streams):
    """
    A syntactic sugar over pipelining ETL. Takes in-, mid- and out-streams and connects them to have data flow in the
    desired pipeline.

    :param instream: in-stream (Extractor) of the pipeline
    :param outstream: out-stream (Loader) of the pipeline
    :param middle_streams: list of mid-stream (Transformers) of the pipeline
    :return:
    """
    for record in instream:
        for middle_stream in middle_streams:
            record = middle_stream.send(record)
        outstream.send(record)
