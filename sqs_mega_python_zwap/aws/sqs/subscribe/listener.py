# IMPORTING STANDARD PACKAGES
import re

from typing import Dict, Union, Optional
from django.conf import settings

# IMPORTING LOCAL PACKAGES
from sqs_mega_python_zwap.aws.sqs.message import SqsMessage
from sqs_mega_python_zwap.aws.sqs.subscribe.api import SqsReceiver


class SqsListener:
    """
    Description: Custom listener function to handle the data from the sqs queue
    """

    __listener: Optional[SqsReceiver]
    __topic_callbacks: Dict[str, callable]
    __all_topics: bool

    def __init__(self, topic_callbacks: Dict[str, callable], all_topics: bool = False,
                 listener: SqsReceiver = None):

        self.__listener = listener
        self.__topic_callbacks = topic_callbacks
        self.__all_topics = all_topics

    @property
    def is_gcloud(self) -> bool:

        try:
            return settings.IS_GCLOUD
        except Exception as e:
            pass
        return False

    def handle_message(self, message: Union[SqsMessage, dict]):

        if self.is_gcloud:
            event_name = message.get("event_name", None)
            event_data = message.get("event_data", {})
            publisher = message.get("publisher", None)
        else:
            event_name = message.payload.event.name
            event_data = message.payload.event.attributes
            publisher = message.payload.event.publisher
        data = {
            "event_data": event_data,
            "publisher": publisher,
            "event_name": event_name
        }
        if self.__all_topics:
            self.__topic_callbacks["*"](data)
        else:
            if event_name is not None:
                keys = self.__topic_callbacks.keys()
                for key in keys:
                    check = re.search(key, event_name) is not None
                    if check:
                        self.__topic_callbacks[key](data)

    def listener(self) -> None:
        """
        Description: Listener function to get the messages and handle with the callback
        """

        if self.is_gcloud is False:
            while True:
                messages = self.__listener.receive_messages()
                for message in messages:
                    self.handle_message(message)
                    self.__listener.delete_message(message)
