from logging import INFO, DEBUG
from typing import Optional

from sqs_mega_python_zwap.aws.payload import MessagePayload, serialize_payload
from sqs_mega_python_zwap.aws.publish import Publisher
from sqs_mega_python_zwap.aws.sqs.api import BaseSqsApi


class SqsPublisher(BaseSqsApi, Publisher):

    def publish(
            self, payload: MessagePayload,
            binary_encoding=False,
            delay_seconds: Optional[int] = None,
            queue_url: Optional[str] = None, **_kwargs
    ) -> str:
        serialized = serialize_payload(payload, binary_encoding=binary_encoding)
        return self.publish_raw_message(serialized,
                                        delay_seconds=delay_seconds,
                                        queue_url=queue_url)

    def publish_raw_message(self, body: str,
                            delay_seconds: Optional[int] = None,
                            queue_url: Optional[str] = None,
                            message_attributes: Optional[dict] = None,
                            **_kwargs) -> str:
        queue_url = self._get_queue_url(queue_url)

        response = self._client.send_message(
            QueueUrl=queue_url,
            MessageBody=body,
            DelaySeconds=delay_seconds,
            MessageAttributes=message_attributes
        )

        message_id = response.get('MessageId')
        self._log_message(INFO, queue_url, message_id, 'Sent SQS message')
        self._log_message(DEBUG, queue_url, message_id, body)
        return message_id
