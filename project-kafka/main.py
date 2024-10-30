from kafka_config import CONSUMER_CONFIG_MAIN, PRODUCER_CONFIG_LOCAL, CONSUMER_CONFIG_LOCAL
from kafka_handler import KafkaMessageHandler
from kafka_postgres_config import POSTGRES_CONFIG
import threading

def forward_messages():
    handler_forward_message = KafkaMessageHandler(
        consumer_config=CONSUMER_CONFIG_MAIN,  
        producer_config=PRODUCER_CONFIG_LOCAL  
    )
    handler_forward_message.kafka_loop(
        source_topic="product_view",
        destination_topic="product_view_local",
        is_forwarding=True
    )

def consume_postgres(consumer_id):
    print(f"Consumer_ID: {consumer_id}")
    handler_consume_postgres = KafkaMessageHandler(
        consumer_config=CONSUMER_CONFIG_LOCAL,  
        postgres_config=POSTGRES_CONFIG
    )
    handler_consume_postgres.kafka_loop(
        local_topic="product_view_local"
    )

if __name__ == "__main__":
    try:
        # Tạo và bắt đầu luồng forward
        thread_forward = threading.Thread(target=forward_messages)
        thread_forward.start()

        # Tạo và bắt đầu 3 luồng consumer
        consumer_threads = []
        for i in range(3):
            thread = threading.Thread(target=consume_postgres, args=(i+1,))
            consumer_threads.append(thread)
            thread.start()

        # Đợi cho tất cả các luồng kết thúc
        thread_forward.join()
        for thread in consumer_threads:
            thread.join()

    except Exception as e:
        print(f"Lỗi xảy ra: {e}")
