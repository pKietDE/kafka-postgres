import logging
from confluent_kafka import Consumer
from handler_postgres import HandlerPostgres
from handler_forward import HandlerForward
from kafka_reader import KafkaReader 
from kafka_config import *
from threading import Thread

# Cấu hình logger
logging.basicConfig(level=logging.INFO)


# Khởi tạo handler cho PostgreSQL
postgres_handler = HandlerPostgres(POSTGRES_CONFIG)

# Khởi tạo handler cho forwarding 
forwarding_handler = HandlerForward(producer_config=PRODUCER_CONFIG_LOCAL)

# Tạo đối tượng KafkaReader cho chế độ xử lý local
local_reader = KafkaReader(CONSUMER_CONFIG_LOCAL, postgres_handler)

# Tạo đối tượng KafkaReader cho chế độ forwarding
forwarding_reader = KafkaReader(CONSUMER_CONFIG_MAIN, forwarding_handler)

def run_local_reader():
    # Đọc message từ topic local và xử lý
    local_reader.kafka_loop(local_topic='product_view_local')
def run_forwarding_reader():
     # Đọc message từ topic nguồn và forward đến topic đích
    forwarding_reader.kafka_loop(source_topic='product_view', destination_topic='product_view_local', is_forwarding=True)

if __name__ == "__main__":
    try:
        # Tạo và khởi động các luồng cho cả hai reader
        local_thread = Thread(target=run_local_reader)
        forwarding_thread = Thread(target=run_forwarding_reader)

        local_thread.start()
        forwarding_thread.start()

        # ngắt chương trình
        local_thread.join()
        forwarding_thread.join()

    except KeyboardInterrupt:
        logging.info("Ngắt chương trình...")

    # Dừng tất cả consumers
    finally:
        local_reader.stop()
        forwarding_reader.stop()
