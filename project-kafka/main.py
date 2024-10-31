import logging
from confluent_kafka import Consumer
from handler_postgres import HandlerPostgres
from handler_forward import HandlerForward
from kafka_reader import KafkaReader 
from threading import Thread
import os

# Cấu hình Consumer từ biến môi trường
CONSUMER_CONFIG_MAIN = {
    'bootstrap.servers': os.getenv('KAFKA_CONSUMER_MAIN_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_CONSUMER_MAIN_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_CONSUMER_MAIN_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_CONSUMER_MAIN_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_CONSUMER_MAIN_SASL_PASSWORD'),
    'group.id': os.getenv('KAFKA_CONSUMER_MAIN_GROUP_ID'),
    'auto.offset.reset': os.getenv('KAFKA_CONSUMER_MAIN_AUTO_OFFSET_RESET')
}

# Cấu hình Producer từ biến môi trường
PRODUCER_CONFIG_LOCAL = {
    'bootstrap.servers': os.getenv('KAFKA_PRODUCER_LOCAL_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_PRODUCER_LOCAL_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_PRODUCER_LOCAL_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_PRODUCER_LOCAL_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_PRODUCER_LOCAL_SASL_PASSWORD'),
}

# Cấu hình Consumer local từ biến môi trường
CONSUMER_CONFIG_LOCAL = {
    'bootstrap.servers': os.getenv('KAFKA_CONSUMER_LOCAL_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_CONSUMER_LOCAL_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_CONSUMER_LOCAL_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_CONSUMER_LOCAL_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_CONSUMER_LOCAL_SASL_PASSWORD'),
    'group.id': os.getenv('KAFKA_CONSUMER_LOCAL_GROUP_ID'),
    'auto.offset.reset': os.getenv('KAFKA_CONSUMER_LOCAL_AUTO_OFFSET_RESET')
}

# Cấu hình PostgreSQL từ biến môi trường
POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DBNAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
}

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
