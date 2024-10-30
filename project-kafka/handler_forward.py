import logging
from confluent_kafka import Producer
from message_handler_interface import MessageHandler

class HandlerForward(MessageHandler):
    def __init__(self, producer_config):
        self.producer_config = producer_config
        self.producer = Producer(self.producer_config)
        self.logger = logging.getLogger(__name__)

    def handle_message(self, msg, destination_topic):
        """Xử lý forwarding message"""
        try:
            message_value = msg.value()
            if isinstance(message_value, bytes):
                message_value = message_value.decode('utf-8')
                
            self.producer.produce(
                destination_topic,
                value=message_value
            )
            self.producer.flush()
            self.logger.info(f"Đã forward message thành công đến {destination_topic}")
        except Exception as e:
            self.logger.error(f"Lỗi khi forward message: {e}")
