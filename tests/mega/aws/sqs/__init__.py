from urllib.parse import parse_qs

import xmltodict


def get_sqs_request_data(cassette):
    request_body = cassette.requests[0].body.decode()
    return parse_qs(request_body)


def get_request_attribute(request_data, attribute_name):
    return request_data[attribute_name][0]


def get_queue_url_from_request(request_data):
    return get_request_attribute(request_data, 'QueueUrl')


def get_sqs_response_body(cassette) -> str:
    return cassette.responses[0]['body']['string'].decode()


def get_sqs_response_data(cassette) -> dict:
    response_body = get_sqs_response_body(cassette)
    return xmltodict.parse(response_body)
