from vcr import VCR

BASE_CASSETTE_LIBRARY_DIR = 'tests/fixtures/cassettes'


def build_vcr(path=None, **kwargs):
    cassette_library_dir = BASE_CASSETTE_LIBRARY_DIR
    if path:
        cassette_library_dir += '/' + path

    settings = dict(
        serializer='yaml',
        cassette_library_dir=cassette_library_dir,
        record_mode='once',
        inject_cassette=True,
        path_transformer=VCR.ensure_suffix('.yml'),
        decode_compressed_response=True,
        filter_headers=[('authorization', '<AUTHORIZATION-HEADER>')],
        match_on=['method', 'scheme', 'host', 'port', 'path', 'query', 'body'],
    )
    settings.update(**kwargs)
    return VCR(**settings)
