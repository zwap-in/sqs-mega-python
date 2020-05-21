import logging

from mega.aws.payload import Payload, serialize_payload
from mega.aws.sqs.api import BaseSqsApi

logger = logging.getLogger('mega.aws.sqs')


class SqsPublishApi(BaseSqsApi):

    def send_message(self, body: str):
        response = self._client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=body,
        )

        message_id = response.get('MessageId')
        logger.info('[{0}][{1}] Sent message'.format(self._queue_url, message_id))
        logger.debug('[{0}][{1}] Message body: {2}'.format(self._queue_url, message_id, body))

    def send_payload(self, payload: Payload, binary_encoding=False):
        serialized = serialize_payload(payload, binary_encoding)
        self.send_message(serialized)
