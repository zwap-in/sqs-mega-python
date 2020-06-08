<p align="center">
    <img alt="Python" src="./resources/logo/sqs-mega-python_logo.png">
</p>

_work in progress..._

---

SQS MEGA is a minimal and fault-tolerant framework for async processing, event-streaming and pattern-matching that uses [Amazon Simple Queue Service (SQS)](https://aws.amazon.com/sqs/). It also implements the [MEGA event protocol](https://github.com/mega-distributed/event-mega). This is the Python library for both publisher and subscriber processes.

Please read the → [SQS MEGA](https://github.com/mega-distributed/sqs-mega) documentation to get started. Here we describe how to use the framework in the **Python** ecosystem.

## AWS configuration

In order for the application to connect to AWS, settings can be automatically read from the IAM environment, or explicitly passed to Python object constructors.

The following authentication settings are needed:

- AWS Access Key ID
- AWS Secret Access Key
- Region name

In order to setup the AWS environment locally for development purposes, you can use the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html). Example:

```
$ aws configure
AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
Default region name [None]: us-west-2
Default output format [None]: ENTER
```

Other settings are needed for reading or writing to SQS queues (such as _queue URL_) or sending SNS notifications (such as _topic ARN_). More on that below.

## Publishing messages

### The `SqsPublisher` class

This class allows you to write messages to a SQS queue.

```python
from mega.aws.sqs.publish import SqsPublisher

publisher = SqsPublisher(
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    region_name='us-west-2',
    queue_url='https://sqs.us-east-2.amazonaws.com/424566909325/sqs-mega-test'
)
```
> ⚠️ The passwords and keys here are just an example, you should never hard-code any secrets in code. Use environment variables or a secret vault for that.

- If `aws_access_key_id`, `aws_secret_access_key` and `region_name` are omitted, they will be read from the IAM environment or AWS CLI configuration.
- The `queue_url` must point to a valid SQS queue URL. Please ensure the IAM user has _write_ permissions to that queue.

### The `SnsPublisher` class

This class allows you to publish notifications to a SNS topic, which might be useful in some situations. For example, you want the same message to be sent to multiple destinations or services.

Please keep in mind that, in order to leverage most of the functionality of this framework, the SNS topic should be configured to also deliver the message to a SQS queue. You can configure SNS to forward the message to a SQS queue in raw format, or the SNS notification can also be embedded inside a SQS message payload, as it is. SQS MEGA is able to automatically detect both types of messages, and payloads are decoded transparently.

```python
from mega.aws.sns.publish import SnsPublisher

publisher = SnsPublisher(
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    region_name='us-west-2',
    topic_arn='arn:aws:sns:us-east-2:424566909325:sqs-mega-test'
)
```
> ⚠️ The passwords and keys here are just an example, you should never hard-code any secrets in code. Use environment variables or a secret vault for that.

- If `aws_access_key_id`, `aws_secret_access_key` and `region_name` are omitted, they will be read from the IAM environment or AWS CLI configuration.
- The `topic_arn` must point to a valid SNS topic ARN. Please ensure the IAM user has _publish_ permissions to the topic.

### Message payloads

A message payload can be one of the following:

- → [_MEGA event_](https://github.com/mega-distributed/event-mega)
- _Data object_: `dict` and JSON objects
- _Plaintext_: `str`
- _Binary blob_: `bytes`

Please read the SQS MEGA documentation about → [message payloads](https://github.com/mega-distributed/sqs-mega#message-payloads).

Both `SqsPublisher` and `SnsPublisher` implement the `publish_payload` method, that will encode and publish any payload type to Amazon SQS and SNS, respectively.

By default, MEGA events and data objects will be serialized to JSON objects and transmitted over plaintext. You can save network bandwidth and server resources by serializing the payloads to [BSON](http://bsonspec.org) (Binary JSON). Since SQS only supports plaintext media, BSON bytes will be transmitted encoded as Base64.

In order to use BSON serialization, set the `binary_encoding` attribute to true when publishing a message.

#### Publishing a data object payload

Any instance of Python's `dict` type is considered a data object. By default, they're serialized to plaintext JSON objects:

```python
payload = {
    'type': 'user_notification',
    'notification_type': 'email',
    'user': {
        'id': 987650,
        'email': 'johndoe_86@example.com'
    }
}

publisher.publish_payload(payload)
```

It's also possible to use BSON to compress the payload, which will be transmitted using more byte-efficient binary encoding:

```python
publisher.publish_payload(payload, binary_encoding=True)
```

#### Publishing a MEGA event payload

The → [MEGA event protocol](https://github.com/mega-distributed/event-mega) aims to be a common protocol for all your event-streaming needs, regardless of platform. Please check the protocol specification for more details.

The `mega.event.PayloadBuilder` class can help you build a MEGA event payload.

```python
from mega.event import PayloadBuilder


builder = PayloadBuilder()
payload = (
    builder.with_event(
        domain='shopping_cart',
        name='item.added',
        subject='987650',
        quantity=5,
        item_id='0794bac2-e860-4e0d-b9cc-42ab21e2a851'
    ).with_object(
        type='shopping_cart',
        id='18a3f92e-1fbf-45eb-8769-d836d0a1be55',
        current={
            'id': '18a3f92e-1fbf-45eb-8769-d836d0a1be55',
            'user_id': 987650,
            'items': [
                {
                    'id': '61fcc874-624e-40f8-8fd7-0e663c7837e8',
                    'price': '19.99',
                    'quantity': 5
                },
                {
                    'id': '3c7f8798-1d3d-47de-82dd-c6c5e0de74ee',
                    'price': '102.50',
                    'quantity': 1
                },
                {
                    'id': 'bba76edc-8afc-4fde-b4c4-ea58a230c5d6',
                    'price': '24.99',
                    'quantity': 3
                }
            ],
            'currency': 'USD',
            'value': '277.42',
            'discount': '10.09',
            'subtotal': '267.33',
            'estimated_shipping': '10.00',
            'estimated_tax': '24.96',
            'estimated_total': '302.29',
            'created_at': '2020-05-03T12:20:23.000',
            'updated_at': '2020-05-04T13:47:08.000'
        }
    ).with_extra(
        channel='web/desktop',
        user_ip_address='177.182.205.103'
    ).build()
)


publisher.publish_payload(payload, binary_encoding=True)
```

#### Plaintext vs. binary encoding

You may be asking: _"should I use binary encoding"_? To understand more, let's see this example:

```python
payload = {
    'type': 'user_notification',
    'notification_type': 'email',
    'user': {
        'id': 987650,
        'email': 'johndoe_86@example.com'
    }
}
```

When encoded as plaintext JSON, it has 118 characters:

```json
{"type": "user_notification", "notification_type": "email", "user": {"id": 987650, "email": "johndoe_86@example.com"}}
```

However, JSON content must be URL-escaped in order to be written to a SQS queue:
```
%7B%22type%22%3A%20%22user_notification%22%2C%20%22notification_type%22%3A%20%22email%22%2C%20%22user%22%3A%20%7B%22id%22%3A%20987650%2C%20%22email%22%3A%20%22johndoe_86%40example.com%22%7D%7D
```
And is delivered XML-escaped when read from a SQS queue:
```
{&quot;type&quot;: &quot;user_notification&quot;, &quot;notification_type&quot;: &quot;email&quot;, &quot;user&quot;: {&quot;id&quot;: 987650, &quot;email&quot;: &quot;johndoe_86@example.com&quot;}}
```
Which consume 192 and 198 characters, respectively.

We can save some bytes by serializing the payload to BSON. However, because SQS only supports plaintext media, binary content must be encoded to Base64. This example takes 156 characters, a ~20% reduction in size:

```
cwAAAAJ0eXBlABIAAAB1c2VyX25vdGlmaWNhdGlvbgACbm90aWZpY2F0aW9uX3R5cGUABgAAAGVtYWlsAAN1c2VyAC8AAAAQaWQAAhIPAAJlbWFpbAAXAAAAam9obmRvZV84NkBleGFtcGxlLmNvbQAAAA==
```

As you can notice, the difference in size for small payloads is negligible. However, BSON over Base64 can offer significant compression benefits when encoding large data objects.

The only downside of using binary encoding is that it makes less clear to inspect queues and see their message contents using the Amazon SQS browser client or the AWS CLI tool. However, you can copy the Base64 string to a Python terminal and decode it using the following command:

```python
>>> import bson
>>> from base64 import b64decode
>>> bson.loads(b64decode('cwAAAAJ0eXBlABIAAAB1c2VyX25vdGlmaWNhdGlvbgACbm90aWZpY2F0aW9uX3R5cGUABgAAAGVtYWlsAAN1c2VyAC8AAAAQaWQAAhIPAAJlbWFpbAAXAAAAam9obmRvZV84NkBleGFtcGxlLmNvbQAAAA=='))
{'type': 'user_notification', 'notification_type': 'email', 'user': {'id': 987650, 'email': 'johndoe_86@example.com'}}
```
> ⚠️ You must install the [`bson`](https://pypi.org/project/bson/) package first.

Even if transmitted using plaintext, JSON content is very difficult for the naked-eye to read in SQS queues because it must be XML or URL escaped in order to fit in the SQS message format. So in order to inspect messages, you must use a tool anyways.

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
