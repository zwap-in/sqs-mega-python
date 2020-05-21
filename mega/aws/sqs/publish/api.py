from logging import INFO, DEBUG
from typing import Optional

from mega.aws.payload import Payload, serialize_payload
from mega.aws.sqs.api import BaseSqsApi


class SqsPublishApi(BaseSqsApi):

    def send_payload(self, payload: Payload, queue_url: Optional[str] = None, binary_encoding=False):
        serialized = serialize_payload(payload, binary_encoding=binary_encoding)
        self.send_raw_message(serialized, queue_url=queue_url)

    def send_raw_message(self, body: str, queue_url: Optional[str] = None):
        queue_url = self._get_queue_url(queue_url)

        response = self._client.send_message(
            QueueUrl=queue_url,
            MessageBody=body,
        )

        message_id = response.get('MessageId')
        self._log(INFO, queue_url, message_id, 'Sent SQS message')
        self._log(DEBUG, queue_url, message_id, body)
