<p align="center">
    <img alt="SQS MEGA" src="https://github.com/sqs-mega/sqs-mega/raw/master/resources/logo/sqs-mega_logo_large.png">
</p>

# PYTHON LIBRARY

**_work in progress ..._**

[SQS MEGA](https://github.com/mega-distributed/sqs-mega) is a minimal framework for async processing, event-streaming and pattern-matching that uses [Amazon Simple Queue Service (SQS)](https://aws.amazon.com/sqs/). It also implements the [MEGA event protocol](https://github.com/mega-distributed/event-mega). This is the Python library for both publisher and subscriber processes.

## Key features

- Publish and consume [MEGA events](https://github.com/mega-distributed/event-mega) and data payloads over [Amazon SQS](https://aws.amazon.com/sqs/) or [Amazon SNS](https://aws.amazon.com/sns/).
- Subscribe to specific events and messages using an expressive **pattern-matching** DSL.
- Scale publisher and subscriber processes independently and at your heart's desire.
- SNS notifications delivered over SQS are decoded transparently.
- Data payloads can be automatically encoded and decoded using [Binary JSON (BSON)](http://bsonspec.org) to save network bandwidth.
- Messages that fail to be processed can be automatically retried.

## Listening to messages

A `SqsListener` object listens to messages from a SQS queue and dispatches them to registered subscribers, in an endless long-polling loop. Subscribers declare pattern-matching rules to determine which messages they are interested about. If a message is matched by a subscriber, the listener will forward the message to it. A message will be forwarded to all subscribers that match it, and the same message may be consumed by many subscribers.

Here is an example:

```python
from mega.aws.sqs.subscribe import SqsListener
from my.app.subscribers import ShoppingCartItemAdded, ShoppingCartItemRemoved, ShoppingCartCheckout

listener = SqsListener(**aws_settings)

listener.register(ShoppingCartItemAdded)
listener.register(ShoppingCartItemRemoved)
listener.register(ShoppingCartCheckout)

listener.listen()
```

In this example, here is how the `ShoppingCartItemAdded` subscriber could look like:

```python
from mega.aws.sqs.subscribe import MegaSubscriber, MegaPayload, Result


class ShoppingCartItemAdded(MegaSubscriber):
    match_event = dict(
        domain='shopping_cart',
        name='item.added',
        version=one_of(1, 2),
        subject=match(r'\d+'),
        item_id=not_(empty()),
        quantity=gt(0)
    )

    validate_object = dict(
        type='shopping_cart',
        id=not_(empty())
    )

    def process(payload: MegaPayload) -> Result:
        cart = ShoppingCart.get(payload.object.id)

        # do something...

        return Result.OK
```

This subscriber will match the following MEGA event payload:

```json
{
    "protocol": "mega",
    "version": 1,
    "event": {
        "domain": "shopping_cart",
        "name": "item.added",
        "version": 2,
        "timestamp": "2020-05-04T15:53:23.123",
        "subject": "987650",
        "publisher": "shopping-cart-service",
        "item_id": "61fcc874-624e-40f8-8fd7-0e663c7837e8",
        "quantity": 5
    },
    "object": {
        "type": "shopping_cart",
        "id": "18a3f92e-1fbf-45eb-8769-d836d0a1be55",
        "current": {
            "id": "18a3f92e-1fbf-45eb-8769-d836d0a1be55",
            "user_id": 987650,
            "items": [
                {
                    "id": "61fcc874-624e-40f8-8fd7-0e663c7837e8",
                    "price": "19.99",
                    "quantity": 10
                },
                {
                    "id": "3c7f8798-1d3d-47de-82dd-c6c5e0de74ee",
                    "price": "102.50",
                    "quantity": 1
                },
                {
                    "id": "bba76edc-8afc-4fde-b4c4-ea58a230c5d6",
                    "price": "24.99",
                    "quantity": 3
                }
            ],
            "currency": "USD",
            "value": "377.37",
            "discount": "20.19",
            "subtotal": "357.18",
            "estimated_shipping": "10.00",
            "estimated_tax": "33.05",
            "estimated_total": "400.23",
            "created_at": "2020-05-03T12:20:23.000",
            "updated_at": "2020-05-04T15:52:01.000"
        },
        "previous": {
            "id": "18a3f92e-1fbf-45eb-8769-d836d0a1be55",
            "user_id": 987650,
            "items": [
                {
                    "id": "61fcc874-624e-40f8-8fd7-0e663c7837e8",
                    "price": "19.99",
                    "quantity": 5
                },
                {
                    "id": "3c7f8798-1d3d-47de-82dd-c6c5e0de74ee",
                    "price": "102.50",
                    "quantity": 1
                },
                {
                    "id": "bba76edc-8afc-4fde-b4c4-ea58a230c5d6",
                    "price": "24.99",
                    "quantity": 3
                }
            ],
            "currency": "USD",
            "value": "277.42",
            "discount": "10.09",
            "subtotal": "267.33",
            "estimated_shipping": "10.00",
            "estimated_tax": "24.96",
            "estimated_total": "302.29",
            "created_at": "2020-05-03T12:20:23.000",
            "updated_at": "2020-05-04T13:47:08.000"
        }
    },
    "extra": {
        "channel": "web/desktop",
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15",
        "user_ip_address": "177.182.205.103"
    }
}
```

Following the same example, the `ShoppingCartItemRemoved` and `ShoppingCartCheckout` subscribers are not interested in this event, so they would not match it.

```python
class ShoppingCartItemRemoved(MegaSubscriber):
    match_event = dict(
        domain='shopping_cart',
        name='item.removed',
        # ...
    )

    def process(payload: MegaPayload) -> Result:
        # ...


class ShoppingCartCheckout(MegaSubscriber):
    match_event = dict(
        domain='shopping_cart',
        name='checkout',
        # ...
    )

    def process(payload: MegaPayload) -> Result:
        # ...
```

After a message is consumed by all interested subscribers, it is deleted from the queue. However, a message may remain in the queue and redelivered later under the following circunstances:

- A subscriber failed to process it with a transient or retriable error, such as network timeout.
- A subscriber processes the message but determines that it must be retried later for any reason. To signal this intent, it should return `SubscriberResult.RETRY_LATER`.
- The listener process dies before the message can be deleted from the queue.

Also, due to the nature of Amazon SQS, messages may be delivered more than once. For these reasons, you should design your subscribers to be idempotent. Read the [best practices for processing asynchronous messages](https://github.com/mega-distributed/sqs-mega#best-practices-for-processing-asynchronous-messages).

Each listener is a blocking thread and must be run in its own process or container. To maximize throughput, you can have many copies of the same process listening to the same queue, as long as the set of subscribers is identical.

Amazon SQS uses the [Visibility Timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html) to minimize race conditions and allow only one process to receive and process a message at a given time. It also uses a shuffling algorithm to distribute messages better between different processes.

> **WARNING**: do **not** allow listener processes with different subscribers to listen to the same queue, otherwise messages might not be delivered to all subscribers, or could be processed incorrectly. You must ensure that all processes that listen to the same queue are identical. If you have multiple copies of a container listening to a queue, you should also keep them up-to-date. Do not allow older containers to share a queue with newer containers.

## Message payloads

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
