import logging
from typing import Optional

import boto3

from sqs_mega_python_zwap.aws.payload import MessagePayload, serialize_payload
from sqs_mega_python_zwap.aws.publish import Publisher

logger = logging.getLogger('mega.aws.sns')


class SnsPublisher(Publisher):

    def __init__(
            self,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            region_name: Optional[str] = None,
            topic_arn: Optional[str] = None
    ):
        self._client = boto3.client(
            'sns',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self._topic_arn = topic_arn

    @property
    def topic_arn(self):
        return self._topic_arn

    def publish(
            self, payload: MessagePayload,
            binary_encoding=False, topic_arn: Optional[str] = None, **_kwargs
    ) -> str:
        serialized = serialize_payload(payload, binary_encoding=binary_encoding)
        return self.publish_raw_message(serialized, topic_arn=topic_arn)

    def publish_raw_message(self, message: str, topic_arn: Optional[str] = None, **_kwargs) -> str:
        topic_arn = self._get_topic_arn(topic_arn)
        event_name = _kwargs.get("event_name", None)
        response = self._client.publish(
            TopicArn=topic_arn,
            Message=message,
            MessageAttributes={
                "event_name": {
                    "DataType": "String",
                    "StringValue": event_name
                }
            }
        )

        message_id = response.get('MessageId')
        logger.info('[{0}][{1}] Published SNS message'.format(topic_arn, message_id))
        logger.debug('[{0}][{1}] {2}'.format(topic_arn, message_id, message))
        return message_id

    def _get_topic_arn(self, override_topic_arn: Optional[str]) -> str:
        topic_arn = override_topic_arn or self._topic_arn

        if not topic_arn:
            raise ValueError('Missing Topic ARN')

        return topic_arn
