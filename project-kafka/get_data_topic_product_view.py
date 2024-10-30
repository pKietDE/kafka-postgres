from confluent_kafka import Consumer, Producer  # type: ignore
import json
from kafka_config import CONSUMER_CONFIG_MAIN, PRODUCER_CONFIG_LOCAL
import logging
from psycopg2 import Error as PostgresError

# Cấu hình log cơ bản
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')

def main():
    # Tạo Consumer để lấy dữ liệu từ topic nguồn
    consumer = Consumer(**CONSUMER_CONFIG_MAIN)

    # Tạo Producer để đẩy dữ liệu vào Kafka local
    producer = Producer(**PRODUCER_CONFIG_LOCAL)

    # Đăng ký topic
    topic = "product_view"
    consumer.subscribe([topic])

    try:
        while True:
            try:
                msg = consumer.poll(2.0)  # Lấy message từ Kafka mỗi 2 giây
                if msg is None:
                    logging.info("Đang đợi message...")
                    continue
                
                if msg.error():
                    logging.error(f"Lỗi Kafka: {msg.error()}")
                    continue

                try:
                    # Xử lý dữ liệu
                    logging.info("Đang xử lý dữ liệu...")
                    producer.produce('product_view_local', value=msg.value().decode('utf-8'))
                    producer.flush()  # Đảm bảo message đã được gửi đi

                except json.JSONDecodeError as e:
                    logging.error(f"Lỗi phân tích JSON: {e}")

            except Exception as e:
                logging.error(f"Lỗi không xác định: {e}")

    except KeyboardInterrupt:
        logging.info("Dừng pull data")
    except Exception as e:
        logging.error(f"Lỗi chính: {e}")
    finally:
        # Đóng các kết nối và consumer
        logging.info("Đóng Consumer, Cursor, Connection")
        if consumer:
            consumer.close()


if __name__ == "__main__":
    main()
