import json
import logging
from confluent_kafka import Consumer, Producer
import psycopg2
from psycopg2 import Error as PostgresError

class KafkaMessageHandler:
    def __init__(self, consumer_config, postgres_config=None, producer_config=None):
        # Thiết lập logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s -%(funcName)s- %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Cấu hình Kafka
        self.consumer_config = consumer_config
        self.producer_config = producer_config
        self.consumer = None
        self.producer = None
        
        # Cấu hình PostgreSQL
        self.postgres_config = postgres_config
        self.conn = None
        self.cursor = None
        self.is_running = True  # Thay đổi giá trị mặc định thành True

        # Khởi tạo consumer
        try:
            self.consumer = Consumer(self.consumer_config)
            self.logger.info("Khởi tạo Kafka consumer thành công")
        except Exception as e:
            self.logger.error(f"Không thể khởi tạo Kafka consumer: {e}")
            raise
        
        # Khởi tạo producer nếu có cấu hình
        if self.producer_config:
            try:
                self.producer = Producer(self.producer_config)
                self.logger.info("Khởi tạo Kafka producer thành công")
            except Exception as e:
                self.logger.error(f"Không thể khởi tạo Kafka producer: {e}")
                raise

    def connect_postgres(self):
        """Thiết lập kết nối PostgreSQL"""
        if not self.postgres_config:
            self.logger.error("Không có cấu hình PostgreSQL")
            return None, None
            
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()
            return conn, cursor
        except PostgresError as e:
            self.logger.error(f"Lỗi kết nối PostgreSQL: {e}")
            raise

    def kafka_loop(self, source_topic=None, local_topic=None, is_forwarding=False, destination_topic=None, poll_timeout=2.0):
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
                    self._handle_forwarding(msg, destination_topic)
                else:
                    self._handle_local_processing(msg)

        except KeyboardInterrupt:
            self.logger.info("Nhận tín hiệu dừng từ người dùng")
        except Exception as e:
            self.logger.error(f"Lỗi trong kafka_loop: {e}")
        finally:
            self.stop()

    def _handle_forwarding(self, msg, destination_topic):
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

    def _handle_local_processing(self, msg):
        """Xử lý message local"""
        try:
            conn, cursor = self.connect_postgres()
            if conn and cursor:
                self.process_message(msg, conn, cursor)
        except Exception as e:
            self.logger.error(f"Lỗi xử lý local message: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def process_message(self, msg, conn, cursor):
        """Xử lý message từ topic local"""
        try:
            value = msg.value().decode('utf-8') if msg.value() else None
            if not value:
                return
                
            value_dict = json.loads(value)
            
            # Lấy các giá trị từ message
            data_to_insert = self._prepare_data_for_insert(value_dict)
            
            # Kiểm tra event_id đã tồn tại
            if not self._is_event_exists(data_to_insert[0], cursor):
                self._insert_data(data_to_insert, conn, cursor)
            else:
                self.logger.info(f"event_id {data_to_insert[0]} đã tồn tại, bỏ qua.")

        except json.JSONDecodeError as e:
            self.logger.error(f"Lỗi phân tích JSON: {e}")
        except Exception as e:
            self.logger.error(f"Lỗi xử lý message: {e}")

    def _prepare_data_for_insert(self, value_dict):
        """Chuẩn bị dữ liệu để insert"""
        return (
            value_dict.get('_id'),
            value_dict.get('time_stamp'),
            value_dict.get('ip'),
            value_dict.get('user_agent'),
            value_dict.get('resolution'),
            value_dict.get('user_id_db'),
            value_dict.get('device_id'),
            value_dict.get('api_version'),
            value_dict.get('store_id'),
            value_dict.get('local_time'),
            value_dict.get('show_recommendation'),
            value_dict.get('current_url'),
            value_dict.get('referrer_url'),
            value_dict.get('email_address'),
            value_dict.get('collection'),
            value_dict.get('product_id') or None,
            self.process_price(value_dict.get('price')),
            value_dict.get('currency'),
            value_dict.get('order_id') or None,
            value_dict.get('is_paypal'),
            value_dict.get('viewing_product_id') or None,
            str(value_dict.get('recommendation')).lower() if value_dict.get('recommendation') is not None else None,

            value_dict.get('utm_source') or None,
            value_dict.get('utm_medium') or None,
            json.dumps(self.process_options(value_dict.get('option', []))),
            json.dumps(self.process_cart_products(value_dict.get('cart_products', []), value_dict.get('collection'))),
            value_dict.get('cat_id') or None,
            value_dict.get('collect_id') or None
        )

    def _is_event_exists(self, event_id, cursor):
        """Kiểm tra event_id đã tồn tại chưa"""
        cursor.execute("SELECT COUNT(*) FROM event_data WHERE event_id = %s", (event_id,))
        return cursor.fetchone()[0] > 0

    def _insert_data(self, data, conn, cursor):
        """Thực hiện insert dữ liệu"""
        insert_query = """
        INSERT INTO event_data (event_id, time_stamp, ip, user_agent, resolution, user_id_db, device_id, api_version,
                            store_id, local_time, show_recommendation, current_url, referrer_url, email_address,
                            collection, product_id, price, currency, order_id, is_paypal, viewing_product_id,
                            recommendation, utm_source, utm_medium, options, cart_products, cat_id, collect_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, data)
        conn.commit()

    def process_options(self, options):
        """Xử lý tùy chọn"""
        # Chuyển đổi danh sách thành dạng mong muốn
        options_list = []
        if not options:
            return []

        if isinstance(options, dict):
            options = [options]

        for option in options:
            option_dict = {
                'option_label': option.get('option_label'),
                'option_id': option.get('value_id'),  
                'value_label': option.get('value_label'),
                'value_id': option.get('value_id'),
                'quality': option.get('quality'),
                'quality_label': option.get('quality_label'),
                'alloy': option.get('alloy'),
                'diamond': option.get('diamond'),
                'shapediamond': option.get('shapediamond'),
            }
            options_list.append(option_dict)
        return options_list

    def process_options_cart_products(self, options):
        """Xử lý tùy chọn cho sản phẩm trong giỏ hàng"""
        # Chuyển đổi danh sách thành dạng mong muốn
        options_list = []
        if not options:
            return []

        if isinstance(options, dict):
            options = [options]

        for option in options:
            option_dict = {
                'option_label': option.get('option_label'),
                'option_id': option.get('value_id'),  
                'value_label': option.get('value_label'),
                'value_id': option.get('value_id'),
                'quality': option.get('quality'),
                'quality_label': option.get('quality_label'),
                'alloy': option.get('alloy'),
                'diamond': option.get('diamond'),
                'shapediamond': option.get('shapediamond'),
            }
            options_list.append(option_dict)
        return options_list

    def process_cart_products(self, cart_products, collection):
        """Xử lý các sản phẩm trong giỏ hàng"""
        # Chuyển đổi danh sách thành dạng mong muốn
        cart_products_list = []
        if not cart_products:
            return []

        for product in cart_products:
            cart_product_dict = {
                'product_id': product.get('product_id'),
                'price': self.process_price(product.get('price')),
                'quantity': product.get('amount'),
                'collection': collection
            }
            cart_products_list.append(cart_product_dict)
        return cart_products_list

    def process_price(self,price_str):
        if price_str is None:
            return None
        try:
            # Loại bỏ dấu phẩy và dấu chấm
            price_str = price_str.replace(',', '').replace('.', '')
            
            # Chỉ giữ lại các ký tự số
            price_str = ''.join(filter(str.isdigit, price_str))
            
            # Loại bỏ 2 chữ số cuối
            price_str = price_str[:-2]
            
            # Chuyển thành số nguyên
            return int(price_str) if price_str else None
        except ValueError:
            print(f"Không thể xử lý giá trị price: {price_str}")
            return None

    def stop(self):
        """Dừng consumer và producer"""
        self.is_running = False
        if self.consumer:
            self.consumer.close()
            self.logger.info("Đã đóng consumer")
        if self.producer:
            self.producer.flush()
            self.logger.info("Đã flush producer")
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Đã đóng kết nối đến PostgreSQL")