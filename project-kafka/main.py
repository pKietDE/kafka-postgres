import logging
from confluent_kafka import Consumer
from handler_postgres import HandlerPostgres
from handler_forward import HandlerForward
from kafka_reader import KafkaReader 
from threading import Thread
from kafka_handle_config import *
import os

# Khoi tao config_reader
config_reader = ConfigReader()

# Cấu hình Consumer từ biến môi trường
CONSUMER_CONFIG_MAIN = config_reader.get_config("KAFKA_CONSUMER_MAIN")
# Cấu hình Producer từ biến môi trường
PRODUCER_CONFIG_LOCAL = config_reader.get_config("KAFKA_PRODUCER_LOCAL")

# Cấu hình Consumer local từ biến môi trường
CONSUMER_CONFIG_LOCAL = config_reader.get_config("KAFKA_CONSUMER_LOCAL")

# Cấu hình PostgreSQL từ biến môi trường
POSTGRES_CONFIG = config_reader.get_config("POSTGRES")

# Cấu hình logger
logging.basicConfig(level=logging.INFO)


# Khởi tạo handler cho PostgreSQL
postgres_handler = HandlerPostgres(POSTGRES_CONFIG)

# Khởi tạo handler cho forwarding 
forwarding_handler = HandlerForward(producer_config=PRODUCER_CONFIG_LOCAL)



# Tạo đối tượng KafkaReader cho chế độ forwarding
forwarding_reader = KafkaReader(CONSUMER_CONFIG_MAIN, forwarding_handler)

def run_local_reader(reader_id):
    print(f"Reader_ID : {reader_id}")

    # Tạo đối tượng KafkaReader cho chế độ xử lý local
    local_reader = KafkaReader(CONSUMER_CONFIG_LOCAL, postgres_handler)

    # Đọc message từ topic local và xử lý
    local_reader.kafka_loop(local_topic='product_view_local')
def run_forwarding_reader():
     # Đọc message từ topic nguồn và forward đến topic đích
    forwarding_reader.kafka_loop(source_topic='product_view', destination_topic='product_view_local', is_forwarding=True)

if __name__ == "__main__":
    try:
        # Tạo và khởi động các luồng cho cả hai reader
        local_threads = []
        for i in range(3):
            thread = Thread(target=run_local_reader, args=(i,))
            local_threads.append(thread)
            thread.start()

        # Khởi động luồng cho forwarding reader
        forwarding_thread = Thread(target=run_forwarding_reader)
        forwarding_thread.start()

        # ngắt chương trình
        forwarding_thread.join()

        # Chờ cho các luồng local hoàn thành
        for thread in local_threads:
            thread.join()

    except KeyboardInterrupt:
        logging.info("Ngắt chương trình...")

    # Dừng tất cả consumers
    finally:
        forwarding_reader.stop()
        postgres_handler.stop()
