import json
import logging
import psycopg2
from psycopg2 import Error as PostgresError
from message_handler_interface import MessageHandler

class HandlerPostgres(MessageHandler):
    def __init__(self, postgres_config):
        self.postgres_config = postgres_config
        self.conn = None
        self.cursor = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """Thiết lập kết nối PostgreSQL"""
        if not self.postgres_config:
            self.logger.error("Không có cấu hình PostgreSQL")
            return None, None
            
        try:
            self.conn = psycopg2.connect(**self.postgres_config)
            self.cursor = self.conn.cursor()
            return self.conn, self.cursor
        except PostgresError as e:
            self.logger.error(f"Lỗi kết nối PostgreSQL: {e}")
            raise

    def handle_message(self, msg):
        """Xử lý message từ topic local"""
        try:
            value = msg.value().decode('utf-8') if msg.value() else None
            if not value:
                return
            
            value_dict = json.loads(value)
            data_to_insert = self._prepare_data_for_insert(value_dict)
            conn, cursor = self.connect()
            if conn and cursor:
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
        self.logger.info("Đẩy dữ liệu lên postgres thành công")
        

    def process_options(self, options):
        """Xử lý tùy chọn"""
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

    def process_price(self, price_str):
        if price_str is None:
            return None
        try:
            price_str = price_str.replace(',', '').replace('.', '')
            price_str = ''.join(filter(str.isdigit, price_str))
            price_str = price_str[:-2]
            return int(price_str) if price_str else None
        except ValueError:
            self.logger.error(f"Không thể xử lý giá trị price: {price_str}")
            return None

    def stop(self):
        """Đóng cursor và kết nối nếu chúng còn mở"""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None
