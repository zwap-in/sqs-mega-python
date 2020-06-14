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

### Sending messages to a SQS queue

The `mega.aws.sqs.publish.SqsPublisher` class allows you to send messages to a SQS queue directly.

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

> ℹ️ If `aws_access_key_id`, `aws_secret_access_key` and `region_name` are omitted, they will be read from the IAM environment or AWS CLI configuration.

The `queue_url` must point to a valid SQS queue URL. Please ensure the IAM user has _write_ permissions to that queue.

### Publishing notifications to a SNS topic

[Amazon SNS](https://aws.amazon.com/sns) is a fully managed pub/sub messaging service that allows you to decouple microservices, distributed systems and serveless applications.

The `mega.aws.sns.publish.SnsPublisher` class allows you to publish notifications to a SNS topic. This is useful if you want to decouple event publishers from subscribers, while delivering the same notification to all parties that are subscribed to a given topic. You can subscribe your SQS queue to the SNS topics that interest your service, and Amazon SNS will forward messages to that queue.

You can configure SNS to forward messages to a SQS queue in raw format. The SNS notification can also be embedded inside a SQS message body. SQS MEGA is able to automatically detect both types of configurations, and message payloads are deserialized in a transparent manner.

```python
from mega.aws.sns import SnsPublisher

publisher = SnsPublisher(
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    region_name='us-west-2',
    topic_arn='arn:aws:sns:us-east-2:424566909325:sqs-mega-test'
)
```
> ⚠️ The passwords and keys here are just an example, you should never hard-code any secrets in code. Use environment variables or a secret vault for that.

> ℹ️ If `aws_access_key_id`, `aws_secret_access_key` and `region_name` are omitted, they will be read from the IAM environment or AWS CLI configuration.

The `topic_arn` must point to a valid SNS topic ARN. Please ensure the IAM user has _publish_ permissions to the topic.

### Message payloads

A message payload can be one of the following:

- → [**MEGA event**](https://github.com/mega-distributed/event-mega) [`mega.event.Payload`]
- **Data object** [`dict`]: JSON objects
- **Plaintext** [`str`]: text that cannot be parsed as JSON (_not supported at the moment_)
- **Binary blob** [`bytes`]: bytes that cannot be decoded as BSON (_not supported at the moment_)

Please read the SQS MEGA documentation about → [message payloads](https://github.com/mega-distributed/sqs-mega#message-payloads).

Both `SqsPublisher` and `SnsPublisher` implement the `publish` method, that will encode and publish any payload type to Amazon SQS and SNS, respectively.

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

publisher.publish(payload)
```

It's also possible to use BSON to compress the payload, which will be transmitted using more byte-efficient binary encoding:

```python
publisher.publish(payload, binary_encoding=True)
```

#### Publishing a MEGA event payload

The → [MEGA event protocol](https://github.com/mega-distributed/event-mega) aims to be a common protocol for all your event-streaming needs, regardless of platform. Please check the protocol specification for more details.

The `mega.event.PayloadBuilder` class can help you build a MEGA event payload.

You can publish a very simple event, like this one:

```python
from mega.event import PayloadBuilder


builder = PayloadBuilder()
payload = (
    builder.with_event(
        name='user.created',
        subject='987650',
        email='johndoe_86@example.com'
    ).with_object(
        current={
            'id': 987650,
            'full_name': 'John Doe',
            'username': 'john.doe',
            'email': 'johndoe_86@example.com',
            'ssn': '497279436',
            'birthdate': '1986-02-15'
        }
    ).build()
)

publisher.publish(payload)
```

You can also create events with complex data and rich payloads:

```python
from mega.event import PayloadBuilder


builder = PayloadBuilder()

builder.with_event(
    domain='shopping_cart',
    name='item.added',
    version=1,
    subject='987650',
    publisher='shopping-cart-service',
    item_id='61fcc874-624e-40f8-8fd7-0e663c7837e8',
    quantity=5,
    price='19.99'
)

builder.with_object(
    type='shopping_cart',
    id='18a3f92e-1fbf-45eb-8769-d836d0a1be55',
    version=2,
    current={
        'id': '18a3f92e-1fbf-45eb-8769-d836d0a1be55',
        'user_id': 987650,
        'items': [
            {
                'id': '61fcc874-624e-40f8-8fd7-0e663c7837e8',
                'price': '19.99',
                'quantity': 10
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
        'value': '377.37',
        'discount': '20.19',
        'subtotal': '357.18',
        'estimated_shipping': '10.00',
        'estimated_tax': '33.05',
        'estimated_total': '400.23',
        'created_at': '2020-05-03T12:20:23.000',
        'updated_at': '2020-05-04T15:52:01.000'
    },
    previous={
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
)

builder.with_extra(
    channel='web/desktop',
    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15',
    user_ip_address='177.182.205.103'
)

payload = builder.build()
publisher.publish(payload, binary_encoding=True)
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
Which consumes 192 and 198 characters, respectively.

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

## Subscribing to messages

### How it works

A `mega.aws.sqs.subscribe.SqsListener` object listens to messages from a SQS queue and dispatches them to registered subscribers, in an endless long-polling loop. Subscribers declare pattern-matching rules to determine which messages they are interested about. If a message is matched by a subscriber, the listener will forward the message to it. A message will be forwarded to all subscribers that match it, and the same message may be consumed by many subscribers.

After a message is consumed by all interested subscribers, it is deleted from the queue. However, a message may remain in the queue and redelivered later in case of transient errors or explicit retries.

Also, due to the nature of Amazon SQS, messages may be delivered more than once. For these reasons, you should design your subscribers to be idempotent. Read the → [best practices for processing asynchronous messages](https://github.com/mega-distributed/sqs-mega#best-practices-for-processing-asynchronous-messages).

Each listener is a blocking thread and must be run in its own process or container. To maximize throughput, you can have many copies of the same process listening to the same queue, as long as those processes are identical.

Amazon SQS uses the [Visibility Timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html) to minimize race conditions and allow only one process to receive and process a message at a given time. It also uses a shuffling algorithm to distribute messages between different processes.

### Example

In this example, we are interested in matching MEGA events that are like:

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
    "object": { ... },
    "extra": { ... }
}
```

First, we must declare a message subscriber. Since this is a MEGA event, not a generic JSON payload, we will use the `mega.aws.sqs.subscribe.EventSubscriber` class:

```python
from mega.aws.sqs.subscribe import EventSubscriber, ProcessStatus
from mega.event import Payload
from mega.match.functions import one_of, gt, not_, empty


class ShoppingCartItemAdded(EventSubscriber):
    domain = 'shopping_cart'
    name = 'item.added'
    version = one_of(1, 2)
    attributes = dict(
        item_id=not_(empty()),
        quantity=gt(0)
    )

    def process(self, payload: Payload) -> ProcessStatus:
        item_id = payload.event.item_id
        item = Inventory.get_item(item_id)
        ...

        return ProcessStatus.DONE
```

We also have other subscribers that match other types of events, namely:

```python
class ShoppingCartItemRemoved(EventSubscriber):
    domain = 'shopping_cart'
    name = 'item.removed'

    def process(self, payload: Payload) -> ProcessStatus:
        ...


class ShoppingCartCheckout(EventSubscriber):
    domain = 'shopping_cart'
    name = 'checkout'

    def process(self, payload: Payload) -> ProcessStatus:
        ...
```

Then, we must instantiate the SQS listener process and register our subscribers:

```python
from mega.aws.sqs.subscribe import SqsListener
...


listener = SqsListener(
    queue_url='https://sqs.us-east-2.amazonaws.com/424566909325/sqs-mega-test',
    ...
)

listener.register_subscriber(ShoppingCartItemAdded)
listener.register_subscriber(ShoppingCartItemRemoved)
listener.register_subscriber(ShoppingCartCheckout)

listener.listen()
```

The listener loop will read messages from the SQS queue and forward them to matching subscribers.

### Message subscribers

A message subscriber is just a set of rules to match and process messages. The base class is `mega.aws.sqs.subscribe.MessageSubscriber`. However, for performing pattern-matching over MEGA events or generic data payloads (i.e., JSON objects), you should use the `mega.aws.sqs.subscribe.EventSubscriber` and `DataSubscriber` classes, respectively.

#### Subscribing to data object payloads

If you have generic JSON payloads sent to your SQS queues, you can subscribe to them by subclassing the `mega.aws.sqs.subscribe.DataSubscriber`:

```python
from mega.aws.sqs.subscribe import DataSubscriber, ProcessStatus
from mega.match.functions import match, one_of, gt, not_

from datetime import date


class UserNotification(DataSubscriber):
    pattern = dict(
        type=match(r'user:notification:(.*)'),
        notification_type=one_of('email', 'sms', 'push'),
        user=dict(
            id=gt(0),
            email=not_(match(r'test@(.*)')),
            template=lambda lhs: lhs and bool(TemplateRepository.get(lhs))
        )
    )

    def process(self, payload: dict) -> ProcessStatus:
        user_id = payload['user']['id']
        ...

        return ProcessStatus.DONE
```

The `pattern` class attribute will determine the pattern rules used to match against a generic data payload. For example, the pattern declared above will match this JSON object:

```json
{
    "type": "user:notification:email",
    "notification_type": "email",
    "ts": 1592163068,
    "user": {
        "id": 987650,
        "first_name": "John",
        "email": "johndoe_86@example.com",
        "template": "password_changed"
    },
    "meta": {
        "generated_by": "user_service",
        "ip_address": "177.182.215.204"
    }
}
```

The pattern must be a Python's dictionary object (`dict`). It will be used to perform partial matching against the data payload received from the SQS Message, which is called the _left-hand side_ (LHS) of the comparison. If the left-hand side data object has additional attributes than declared in the pattern's `dict` (the comparison's _right-hand side_, or RHS), these extra attributes will be ignored. However, if the _right-hand side_ pattern specifies more attributes than the _left-hand side_ payload has, they will not match.

Thus, the following data payload will **not** match the pattern from the example above:

```json
{
    "type": "user:notification:email",
    "notification_type": "email"
}
```

> ⚠️ **WARNING**: the `pattern` class attribute is optional. In this case, the subscriber will match and process all data object payloads that are sent to a SQS queue. This can be useful in some scenarios, for example if you want to implement your custom pattern-match logic or forward messages to another system. But its inadvertent usage can have unintended consequences.

#### Subscribing to MEGA events

Just declare a subclass of `mega.aws.sqs.subscribe.EventSubscriber`, like in the `ShoppingCartItemAdded` example above.

Pattern matching rules are declared through class attributes. Here is another example:

```python
import mega.event
from mega.aws.sqs.subscribe import EventSubscriber, ProcessStatus
from mega.match.functions import match, any_, not_, empty, gt, gte, one_of

from datetime import date


class UserLogin(EventSubscriber):
    domain = 'user'
    name = match(r'user:login:(.*)')
    version = any_()
    subject = match(r'^\d+$')
    attributes = dict(
        success=True,
        attempts=gt(2)
    )

    object_type = 'user'
    object_version = one_of(1, 2, 3)
    object_current = dict(
        email=not_(empty()),
        username=not_(['test', 'synthetic']),
        created_at=gte(date(2020, 1, 1))
    )

    def process(self, payload: mega.event.Payload) -> ProcessStatus:
        user_id = payload.event.subject
        ...

        return ProcessStatus.DONE
```

This will match the following MEGA event payload:

```json
{
    "protocol": "mega",
    "version": "1",
    "event": {
        "domain": "user",
        "name": "user:login:web",
        "version": 1,
        "timestamp": "2020-06-12T17:29:58.000",
        "subject": "987650",
        "publisher": "login-api",
        "attributes": {
            "email": "johndoe_86@example.com",
            "success": true,
            "attempts": 4
        }
    },
    "object": {
        "type": "user",
        "id": "987650",
        "version": 2,
        "current": {
            "id": 987650,
            "username": "john-doe",
            "email": "johndoe_86@example.com",
            "created_at": "2020-03-28T15:23:01",
            "updated_at": "2020-05-18T16:12:59",
            "last_login": "2020-06-02T05:38:43"
        }
    },
    "extra": {
        "ip_address": "2804:14c:5ba9:b067:70cc:9baa:5287:19b3",
        "fingerprint": "MmRkMDczMmItMWJlOS00ZjRmLThlYjEtNmJhNjhjMmY1OWMzCg=="
    }
}
```

Here is a list of all supported pattern-matching class attributes that can be declared in `mega.aws.sqs.subscribe.EventSubscriber`.

|  `EventSuscriber` Class Attribute | Alias             | Value Type            | MEGA payload path  |
| --------------------------------- | ----------------- | --------------------- | ------------------ |
| `event_name`                      | `name`            | String                | `event.name`       |
| `event_domain`                    | `domain`          | String                | `event.domain`     |
| `event_version`                   | `version`         | Number (`int`)        | `event.version`    |
| `event_timestamp`                 | `timestamp`       | DateTime (`datetime`) | `event.timestamp`  |
| `event_subject`                   | `subject`         | String                | `event.subject`    |
| `event_publisher`                 | `publisher`       | String                | `event.publisher`  |
| `event_attributes`                | `attributes`      | Mapping (`dict`)      | `event.attributes` |
| `object_type`                     | -                 | String                | `object.type`      |
| `object_version`                  | -                 | Number (`int`)        | `object.version`   |
| `object_id`                       | -                 | String                | `object.id`        |
| `object_current`                  | `current_object`  | Mapping (`dict`)      | `object.current`   |
| `object_previous`                 | `previous_object` | Mapping (`dict`)      | `object.previous`  |

> ℹ️ Some of these attributes have aliases. So declaring `event_name` or just `name` has the same meaning.

> ⚠️ If both the class attribute and its alias are declared in `EventSubscriber`, the alias will be ignored.

> ⚠️ **WARNING**: these pattern-match attributes are optional, and you should be able to declare any pattern combination you want. However, please be careful because it is also possible to declare a subscriber with no matching rules. In that case, the subscriber will match and process all events that are sent to a SQS queue. This can be useful in some scenarios, for example if you want to implement your custom pattern-match logic or forward messages to another system. But its inadvertent usage can have unintended consequences.

#### Processing payloads

A subscriber must declare a `process` method, which is intended for consuming and processing the payload. After the payload is processed, you are expected to return a value from the `mega.aws.sqs.subscribe.ProcessStatus` enumeration:

|  `ProcessStatus` | Description |
| --------------------- | ----------- |
|  `DONE` | The payload was successfully processed, without any errors. The message will be deleted from the queue and not processed again, if no more subscribers match it. This is also the **default** status, in case nothing is returned by the `process` method. |
|  `RETRY_LATER` | The system was not ready to process the payload yet. The message will remain in the queue and reprocessed later. This is not considered an error. |
|  `FATAL_ERROR` | A fatal error happened when processing the payload. The message will be deleted from the queue and not processed again, if no more subscribers match it. This is also the default status if any generic exception is raised that is not mapped as retriable. |
|  `RETRIABLE_ERROR` | A transient or retriable error happened when processing the payload. The message will remain in the queue and reprocessed later. This is also the status that is assumed if a retriable exception is raised. See how to map exceptions as retriable below. |

Here's an example:

```python
import mega.event
from mega.aws.sqs.subscribe import EventSubscriber, ProcessStatus


class ShoppingCartCheckout(EventSubscriber):
    ...

    def process(payload: mega.event.Payload) -> ProcessStatus:
        lock_name = 'shopping_cart.checkout:' + str(payload.object.id)

        try:
            with DatabaseLock(lock_name):
                ...

                return ProcessStatus.DONE

        except DatabaseLockAlreadyAcquired:
            return ProcessStatus.RETRY_LATER
```

Please be aware that Amazon SQS will ocasionally deliver duplicate messages, even shortly after being processed. Also, if multiple subscribers match the same message, when one subscriber retries processing with `ProcessStatus.RETRY_LATER` or a retriable exception, the message will be redelivered to all matching subscribers again, even those that already processed the message successfully.

> ⚠️ **Design your subscribers to be idempotent**. Please read the → [best practices for processing asynchronous messages](https://github.com/mega-distributed/sqs-mega#best-practices-for-processing-asynchronous-messages).

### SQS listener

The `mega.aws.sqs.subscribe.SqsListener` listens to messages from a SQS queue and dispatches them to registered subscribers, in an endless long-polling loop.

```python
from mega.aws.sqs.subscribe import SqsListener

listener = SqsListener(
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    region_name='us-west-2',
    queue_url='https://sqs.us-east-2.amazonaws.com/424566909325/sqs-mega-test',
    max_number_of_messages=1,
    wait_time_seconds=20,
    visibility_timeout=30
)
```
> ⚠️ The passwords and keys here are just an example, you should never hard-code any secrets in code. Use environment variables or a secret vault for that.

> ℹ️ If `aws_access_key_id`, `aws_secret_access_key` and `region_name` are omitted, they will be read from the IAM environment or AWS CLI configuration.

The following attributes correspond to the attributes passed to the Amazon SQS [`ReceiveMessage`](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) API:

|  Attribute               | SQS Attribute         | Description | Default |
| ------------------------ | --------------------- | ----------- | ------- |
| `queue_url`              | `QueueUrl`            | The URL of the Amazon SQS queue from which messages are received. Please ensure the IAM user has both _read_ and _delete_ permissions to that queue. | - |
| `max_number_of_messages` | `MaxNumberOfMessages` | The maximum number of messages to return. Amazon SQS never returns more messages than this value (however, fewer messages might be returned). It can range from 1 to 10. | 1 |
| `wait_time_seconds`      | `WaitTimeSeconds`     | The duration (in seconds) for which the call waits for a message to arrive in the queue before returning. If a message is available, the call returns sooner than `WaitTimeSeconds`. If no messages are available and the wait time expires, the call returns successfully with an empty list of messages. Please read the [Short and Long Polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html) section from the Amazon SQS Developer Guide in order to configure this attribute correctly. | 20 |
| `visibility_timeout`     | `VisibilityTimeout`   | The duration (in seconds) that the received messages are hidden from subsequent retrieve requests after being retrieved by a `ReceiveMessage` request. Please read the [SQS Visibility Timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html) section from the Amazon SQS Developer Guide in order to configure this attribute correctly. | 30 |

#### Registering message subscribers

The `register_subscriber` method from `SqsListener` allows message and event subscribers to be registered to the listener:

```python
listener.register_subscriber(ShoppingCartItemAdded)
listener.register_subscriber(ShoppingCartItemRemoved)
listener.register_subscriber(ShoppingCartCheckout)
```

A subscriber must be a subclass of `mega.aws.sqs.subscribe.MessageSubscriber`, `DataSubscriber` or `EventSubscriber`. You must register a subscriber class (_type_), and the constructor should require no parameters to be instantiated.

> ℹ️ A subscriber type can only be registered once. The reason is obvious: it does not make any sense to register the same subscriber type, like the `ShoppingCartItemAdded` above, twice. A subscriber is just a set of rules that match and process messages.

#### Running the message loop

After an instance of `SqsListener` has been correctly configured, and all subscribers registered to it, then the `listen` method will start to listen and process messages from a SQS queue in an endless long-polling loop. This will block the current thread indefinitely.

```python
listener.listen()
```

We recommend that each SQS listener is run in its own process instance. If possible, try to run them in different Docker containers. This will allow you to scale your listener processes with more safety and flexibility.

You can have many SQS listener process or container instances listening to the same SQS queue, but please ensure the process or Docker images are **identical**.

> ⚠️ **WARNING**: do not allow different listener process images to listen to the same queue, otherwise messages will be lost or processed incorrectly. You must ensure that all processes that listen to the same queue are identical. If you have multiple instances of a container listening to a queue, you should also keep them up-to-date. Do not allow older containers to share a queue with newer containers. The easiest way to accomplish this is always deploying one Docker image per SQS queue, and bootstrapping any number of identical containers from it.

> ℹ️ _Hint_: you can use [Supervisor](http://supervisord.org) to ensure that the SQS listener process is automatically restarted in case it dies.
