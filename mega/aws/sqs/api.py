import logging
from abc import ABC
from typing import Optional

import boto3

from mega.aws.sqs import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class BaseSqsApi(ABC):

    def __init__(
            self,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            region_name: Optional[str] = None,
            queue_url: Optional[str] = None
    ):
        self._client = boto3.client(
            'sqs',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self._queue_url = queue_url

    @property
    def queue_url(self):
        return self._queue_url

    def _get_queue_url(self, override_queue_url: Optional[str]) -> str:
        queue_url = override_queue_url or self.queue_url
        if not queue_url:
            raise ValueError('Missing Queue URL')
        return queue_url

    @staticmethod
    def _log(level, queue_url: str, text: str):
        logger.log(level, '[{0}] {1}'.format(queue_url, text))

    @classmethod
    def _log_message(cls, level, queue_url: str, message_id: str, text: str):
        logger.log(level, '[{0}][{1}] {2}'.format(queue_url, message_id, text))
