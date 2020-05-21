import logging
from typing import Optional

import boto3

from mega.aws.payload import Payload, serialize_payload
from mega.aws.sns import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class SnsPublishApi:

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

    def publish_payload(self, payload: Payload, topic_arn: Optional[str] = None, binary_encoding=False):
        serialized = serialize_payload(payload, binary_encoding)
        self.publish_raw_message(serialized, topic_arn=topic_arn)

    def publish_raw_message(self, message: str, topic_arn: Optional[str] = None):
        topic_arn = self._get_topic_arn(topic_arn)

        response = self._client.publish(
            TopicArn=topic_arn,
            Message=message
        )

        message_id = response.get('MessageId')
        logger.info('[{0}][{1}] Published SNS message'.format(topic_arn, message_id))
        logger.debug('[{0}][{1}] {2}'.format(topic_arn, message_id, message))

    def _get_topic_arn(self, override_topic_arn: str):
        topic_arn = override_topic_arn or self._topic_arn

        if not topic_arn:
            raise ValueError('Missing Topic ARN')

        return topic_arn
