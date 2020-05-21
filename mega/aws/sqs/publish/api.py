from logging import INFO, DEBUG

from mega.aws.payload import Payload, serialize_payload
from mega.aws.sqs.api import BaseSqsApi


class SqsPublishApi(BaseSqsApi):

    def send_payload(self, payload: Payload, binary_encoding=False):
        serialized = serialize_payload(payload, binary_encoding)
        self.send_raw_message(serialized)

    def send_raw_message(self, body: str):
        response = self._client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=body,
        )

        message_id = response.get('MessageId')
        self._log(INFO, message_id, 'Sent SQS message')
        self._log(DEBUG, message_id, body)
