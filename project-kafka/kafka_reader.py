import logging
from confluent_kafka import Consumer
from handler_postgres import HandlerPostgres
from handler_forward import HandlerForward
from message_handler_interface import *

class KafkaReader:
    def __init__(self, consumer_config, handler: MessageHandler):
        self.logger = logging.getLogger(__name__)
        self.consumer = Consumer(consumer_config)
        self.handler = handler
        self.is_running = True

    def kafka_loop(self,local_topic=None, source_topic=None,  is_forwarding=False, destination_topic=None, poll_timeout=2.0):
        """Vòng lặp chung để tiêu thụ thông điệp"""
        
        if is_forwarding and not (source_topic and destination_topic):
            self.logger.error("Thiếu source_topic hoặc destination_topic cho chế độ forwarding")
            return
            
        if not is_forwarding and not local_topic:
            self.logger.error("Thiếu local_topic cho chế độ xử lý local")
            return

        self.consumer.subscribe([source_topic if is_forwarding else local_topic])
        
        try:
            while self.is_running:
                msg = self.consumer.poll(poll_timeout)

                if msg is None:
                    self.logger.info("Đang đợi message...")
                    continue

                if msg.error():
                    self.logger.error(f"Lỗi Consumer: {msg.error()}")
                    continue

                if is_forwarding:
                    self.handler.handle_message(msg, destination_topic)
                else:
                    self.handler.handle_message(msg, self.consumer)

        except KeyboardInterrupt:
            self.logger.info("Nhận tín hiệu dừng từ người dùng")
        except Exception as e:
            self.logger.error(f"Lỗi trong kafka_loop: {e}")
        finally:
            self.stop()

    def stop(self):
        """Dừng consumer"""
        self.is_running = False
        if self.consumer:
            self.consumer.close()
            self.logger.info("Đã đóng consumer")
