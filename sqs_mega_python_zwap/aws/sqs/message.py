from typing import Optional

from sqs_mega_python_zwap.aws.message import Message, PayloadType, MessagePayload, MessageType


class SqsMessage(Message):
    def __init__(
            self,
            message_id: str,
            receipt_handle: str,
            payload: Optional[MessagePayload],
            payload_type: PayloadType,
            embedded_message: Optional[Message] = None
    ):
        self._message_id = message_id
        self._receipt_handle = receipt_handle
        self._payload = payload
        self._payload_type = payload_type
        self._embedded_message = embedded_message

    @property
    def message_id(self) -> str:
        return self._message_id

    @property
    def message_type(self) -> MessageType:
        return MessageType.SQS

    @property
    def payload_type(self) -> PayloadType:
        return self._payload_type

    @property
    def payload(self) -> Optional[MessagePayload]:
        return self._payload

    @property
    def embedded_message(self) -> Optional[Message]:
        return self._embedded_message

    @property
    def receipt_handle(self) -> str:
        return self._receipt_handle
