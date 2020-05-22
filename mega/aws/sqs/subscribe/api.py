from logging import DEBUG, INFO
from typing import List, Optional

from mega.aws.sqs.api import BaseSqsApi
from mega.aws.sqs.message import SqsMessage
from mega.aws.sqs.schema import deserialize_sqs_message


class SqsReceiver(BaseSqsApi):

    def __init__(
            self,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            region_name: Optional[str] = None,
            queue_url: Optional[str] = None,
            max_number_of_messages: int = 1,
            wait_time_seconds: int = 20,
            visibility_timeout: int = 30
    ):
        super().__init__(
            aws_access_key_id,
            aws_secret_access_key,
            region_name,
            queue_url
        )

        self._max_number_of_messages = max_number_of_messages
        self._wait_time_seconds = wait_time_seconds
        self._visibility_timeout = visibility_timeout

    @property
    def max_number_of_messages(self) -> int:
        return self._max_number_of_messages

    @property
    def wait_time_seconds(self) -> int:
        return self._wait_time_seconds

    @property
    def visibility_timeout(self) -> int:
        return self._visibility_timeout

    def receive_messages(self) -> List[SqsMessage]:
        response = self._client.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=self._max_number_of_messages,
            WaitTimeSeconds=self._wait_time_seconds,
            VisibilityTimeout=self._visibility_timeout
        )

        if 'Messages' not in response:
            self._log(DEBUG, 'No messages received')
            return []

        return self.__extract_messages(response)

    def __extract_messages(self, response):
        messages = []
        for data in response['Messages']:
            self.__log_message_data(data)
            messages.append(deserialize_sqs_message(data))
        return messages

    def __log_message_data(self, data):
        message_id = data.get('MessageId')
        self._log(INFO, message_id, 'Received message')
        self._log(DEBUG, message_id, data.get('Body'))

    def delete_message(self, message: SqsMessage):
        self._client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=message.receipt_handle
        )

        self._log(INFO, message.message_id, 'Deleted message')
