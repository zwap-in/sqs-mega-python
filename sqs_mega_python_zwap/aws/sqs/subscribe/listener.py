# IMPORTING STANDARD PACKAGES
from typing import Dict

# IMPORTING LOCAL PACKAGES
from sqs_mega_python_zwap.aws.sqs.message import SqsMessage
from sqs_mega_python_zwap.aws.sqs.subscribe.api import SqsReceiver


class SqsListener:
    """
    Description: Custom listener function to handle the data from the sqs queue
    """

    __listener: SqsReceiver
    __topic_callbacks: Dict[str, callable]
    __all_topics: bool

    def __init__(self, listener: SqsReceiver, topic_callbacks: Dict[str, callable], all_topics: bool = False):

        self.__listener = listener
        self.__topic_callbacks = topic_callbacks
        self.__all_topics = all_topics

    def handle_message(self, message: SqsMessage):

        event_name = message.payload.event.name
        event_data = message.payload.event.attributes
        publisher = message.payload.event.publisher
        data = {
            "event_data": event_data,
            "publisher": publisher,
            "event_name": event_name
        }
        if event_name in self.__topic_callbacks.keys():
            self.__topic_callbacks[event_name](data)
        elif self.__all_topics:
            self.__topic_callbacks["*"](data)

    def listener(self) -> None:
        """
        Description: Listener function to get the messages and handle with the callback
        """

        while True:
            messages = self.__listener.receive_messages()
            for message in messages:
                self.handle_message(message)
                self.__listener.delete_message(message)
