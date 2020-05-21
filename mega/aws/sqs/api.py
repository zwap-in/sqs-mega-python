import logging
from abc import ABC
from typing import Optional

import boto3

logger = logging.getLogger('mega.aws.sqs')


class BaseSqsApi(ABC):

    def __init__(
            self,
            queue_url: str,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            region_name: Optional[str] = None
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

    def _log(self, level, message_id=None, msg=None):
        if message_id:
            logger.log(level, '[{0}][{1}] {2}'.format(self._queue_url, message_id, msg))
        else:
            logger.log(level, '[{0}] {1}'.format(self._queue_url, msg))
