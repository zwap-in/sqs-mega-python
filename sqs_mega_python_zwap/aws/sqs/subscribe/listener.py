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

    def __init__(self, listener: SqsReceiver, topic_callbacks: Dict[str, callable]):

        self.__listener = listener
        self.__topic_callbacks = topic_callbacks

    def handle_message(self, message: SqsMessage):

        publisher = message.payload.event.publisher
        topic = message.payload.event.subject
        event_data = message.payload.event.attributes
        if topic in self.__topic_callbacks.keys():
            data = {
                "publisher": publisher,
                "event_data": event_data
            }
            self.__topic_callbacks[topic](data)

    def listener(self) -> None:
        """
        Description: Listener function to get the messages and handle with the callback
        """

        while True:
            messages = self.__listener.receive_messages()
            for message in messages:
                self.handle_message(message)
                self.__listener.delete_message(message)
