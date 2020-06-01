<p align="center">
    <img alt="SQS MEGA" src="https://github.com/sqs-mega/sqs-mega/raw/master/resources/logo/sqs-mega_logo_large.png">
</p>

# PYTHON LIBRARY

**_work in progress ..._**

[SQS MEGA](https://github.com/mega-distributed/sqs-mega) is a minimal framework for async processing, event-streaming and pattern-matching that uses Amazon Simple Queue Service (SQS). It also implements the [MEGA event protocol](https://github.com/mega-distributed/event-mega). This is the Python library for both publisher and subscriber processes.

## KEY FEATURES

- Publish and consume [MEGA events](https://github.com/mega-distributed/event-mega) and data payloads over [Amazon SQS](https://aws.amazon.com/sqs/) or [Amazon SNS](https://aws.amazon.com/sns/).
- Subscribe to specific events and messages using an expressive **pattern-matching** DSL.
- Scale publisher and subscriber processes independently and at your heart's desire.
- SNS notifications delivered over SQS are decoded transparently.
- Data payloads can be automatically encoded and decoded using [Binary JSON (BSON)](http://bsonspec.org) to save network bandwidth.
- Messages that fail to be processed can be automatically retried.

## MESSAGE PAYLOADS

A payload can have one of the following types:

- [MEGA event](https://github.com/mega-distributed/event-mega)
- Data object (dictionary)
- Plaintext
- Binary blob

Currently, only JSON is supported as an underlying format for both data objects and MEGA events. Optionally, [Binary JSON (BSON)](http://bsonspec.org) can also be used to compress messages over the network.

Because SQS messages can only be transmitted over plaintext media, binary content such as BSON or bytes will be automatically encoded by SQS MEGA using [Base64](https://en.wikipedia.org/wiki/Base64).

Any string that cannot be decoded as Base64 or JSON is considered plaintext with Unicode encoding. Similarly, any set of bytes encoded as Base64 that cannot be decoded as BSON will be considered a generic binary blob.

SQS MEGA gives message subscribers the ability to perform pattern-matching, but that only applies to MEGA events and data objects. If a message contains a plaintext payload that cannot be decoded as JSON or a binary payload that cannot be decoded as BSON, SQS MEGA allows you to either:

- Provide custom code to consume the plaintext or binary payload
- Provide custom code to transform the plaintext or binary payload to a data object (dictionary), which then can have pattern-matching rules applied.
- Ignore the unrecognized plaintext or binary payload and delete the message from the queue (default).

## PATTERN-MATCHING DSL

Suppose the following MEGA event is published:

```json
{
    "protocol": "mega",
    "version": 1,
    "event": {
        "name": "user:updated",
        "timestamp": "2020-05-04T15:53:27.823",
        "version": 2,
        "domain": "user",
        "subject": "987650",
        "publisher": "user-service",
        "attributes": {
            "email": "johndoe_84@example.com",
            "username": "john.doe"
        }
    },
    "object": {
        "current": {
            "id": 987650,
            "full_name": "John Doe",
            "username": "john.doe",
            "email": "johndoe_86@example.com",
            "ssn": "497279436",
            "birthdate": "1986-02-15",
            "created_at": "2020-05-03T12:20:23.000",
            "updated_at": "2020-05-04T15:52:01.000"
        }
    }
}
```

We can have the following subscriber matching and consuming this event:

```python
class UserSubscriber(MegaSubscriber):

    match_event = {
        'name': match(r'user:(.*)'),
        'version': one_of(1, 2, 3),
        'timestamp': gt(DateTime('2020-01-01')),
        'domain': 'user',
        'subject': match(r'\d+'),
        'email': not_(empty()),
        'username': not_(one_of('synthetic', 'test')),
    }

    def validates(payload: MegaPayload, **kwargs):
        current_object = payload.object.current
        assert current_object['id'] is not None

    def process(payload: MegaPayload, **kwargs):
        user = User.get(int(payload.subject))

        if not user:
            return SubscriberResult.RETRY

        # do something
```
