from logging import INFO, DEBUG
from typing import Optional

from mega.aws.payload import Payload, serialize_payload
from mega.aws.publish import Publisher
from mega.aws.sqs.api import BaseSqsApi


class SqsPublisher(BaseSqsApi, Publisher):

    def publish_payload(self, payload: Payload, binary_encoding=False, queue_url: Optional[str] = None, **_) -> str:
        serialized = serialize_payload(payload, binary_encoding=binary_encoding)
        return self.publish_raw_message(serialized, queue_url=queue_url)

    def publish_raw_message(self, body: str, queue_url: Optional[str] = None, **_) -> str:
        queue_url = self._get_queue_url(queue_url)

        response = self._client.send_message(
            QueueUrl=queue_url,
            MessageBody=body,
        )

        message_id = response.get('MessageId')
        self._log_message(INFO, queue_url, message_id, 'Sent SQS message')
        self._log_message(DEBUG, queue_url, message_id, body)
        return message_id
