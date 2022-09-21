from logging import DEBUG, INFO
from typing import List, Optional

from sqs_mega_python_zwap.aws.sqs.api import BaseSqsApi
from sqs_mega_python_zwap.aws.sqs.message import SqsMessage
from sqs_mega_python_zwap.aws.sqs.schema import deserialize_sqs_message


class SqsReceiver(BaseSqsApi):

    def __init__(
            self,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            region_name: Optional[str] = None,
            queue_url: Optional[str] = None,
            max_number_of_messages: int = 1,
            wait_time_seconds: int = 1,
            visibility_timeout: int = 1
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

    def receive_messages(
            self,
            queue_url: Optional[str] = None,
            max_number_of_messages: Optional[int] = None,
            wait_time_seconds: Optional[int] = None,
            visibility_timeout: Optional[int] = None
    ) -> List[SqsMessage]:
        queue_url = self._get_queue_url(queue_url)
        max_number_of_messages = \
            self._max_number_of_messages if max_number_of_messages is None else max_number_of_messages
        wait_time_seconds = self._wait_time_seconds if wait_time_seconds is None else wait_time_seconds
        visibility_timeout = self._visibility_timeout if visibility_timeout is None else visibility_timeout

        response = self.receive_raw_messages(queue_url, max_number_of_messages, wait_time_seconds, visibility_timeout)

        if 'Messages' not in response:
            self._log(DEBUG, queue_url, 'No messages received')
            return []

        return self.__extract_messages(queue_url, response)

    def receive_raw_messages(self, queue_url, max_number_of_messages, wait_time_seconds, visibility_timeout):
        self._log(
            DEBUG, queue_url,
            'Querying messages. MaxNumberOfMessages={}; WaitTimeSeconds={}; VisibilityTimeout={}'.format(
                max_number_of_messages, wait_time_seconds, visibility_timeout
            )
        )
        response = self._client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_number_of_messages,
            WaitTimeSeconds=wait_time_seconds,
            VisibilityTimeout=visibility_timeout,
            MessageAttributeNames=[
                'All'
            ]
        )
        return response

    def __extract_messages(self, queue_url, response):
        messages = []
        for data in response['Messages']:
            self.__log_message_data(queue_url, data)
            sqs_message = deserialize_sqs_message(data)
            messages.append(sqs_message)
        return messages

    def __log_message_data(self, queue_url, data):
        message_id = data.get('MessageId')
        self._log_message(INFO, queue_url, message_id, 'Received message')
        self._log_message(DEBUG, queue_url, message_id, data.get('Body'))

    def delete_message(self, message: SqsMessage, queue_url: Optional[str] = None):
        queue_url = self._get_queue_url(queue_url)

        self._client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message.receipt_handle
        )

        self._log_message(INFO, queue_url, message.message_id, 'Deleted message')
        self._log_message(DEBUG, queue_url, message.message_id, 'ReceiptHandle={}'.format(message.receipt_handle))
