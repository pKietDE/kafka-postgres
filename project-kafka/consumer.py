from confluent_kafka import Consumer  # type: ignore
import json
import psycopg2  # type: ignore
from postgres_config import POSTGRES_CONFIG
from consumer_config import CONSUMER_CONFIG
import logging
from psycopg2 import Error as PostgresError

# Cấu hình log cơ bản 
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')

def connect_postgres():
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        return conn, cursor
    except PostgresError as e:
        logging.error(f"Lỗi kết nối PostgreSQL: {e}")
        raise

def main():
    # Tạo Consumer
    consumer = Consumer(**CONSUMER_CONFIG)

    # Đăng ký topic 
    topic = "product_view"
    consumer.subscribe([topic])

    conn = None
    cursor = None
    
    try:
        conn, cursor = connect_postgres()
        
        while True:
            try:
                msg = consumer.poll(2.0)
                if msg is None:
                    logging.info("Đang đợi message...")
                    continue
                
                if msg.error():
                    logging.error(f"Lỗi Kafka: {msg.error()}")
                    continue

                value = msg.value().decode('utf-8') if msg.value() else None
                
                try:
                    logging.info("Đang xử lý dữ liệu...")
                    # Phân tích JSON nếu value là chuỗi JSON
                    value_dict = json.loads(value) if value else {}

                    # Lấy các giá trị
                    event_id = value_dict.get('_id')
                    time_stamp = value_dict.get('time_stamp')
                    ip = value_dict.get('ip')
                    user_agent = value_dict.get('user_agent')
                    resolution = value_dict.get('resolution')
                    user_id_db = value_dict.get('user_id_db')
                    device_id = value_dict.get('device_id')
                    api_version = float(value_dict.get('api_version'))
                    store_id = value_dict.get('store_id')
                    local_time = value_dict.get('local_time')
                    show_recommendation = value_dict.get('show_recommendation')
                    current_url = value_dict.get('current_url')
                    referrer_url = value_dict.get('referrer_url')
                    email_address = value_dict.get('email_address')
                    collection = value_dict.get('collection')
                    product_id = value_dict.get('product_id')
                    price = process_price(value_dict.get('price'))
                    currency =  value_dict.get('currency')
                    order_id = value_dict.get('order_id')
                    is_paypal = value_dict.get('is_paypal')
                    viewing_product_id = value_dict.get('viewing_product_id')
                    recommendation = value_dict.get('recommendation')
                    utm_source = value_dict.get('utm_source')
                    utm_medium = value_dict.get('utm_medium')
                    options = process_options(value_dict.get('option',[])) 
                    cart_products = process_cart_products(value_dict.get('cart_products',[]), value_dict.get('collection'))
                    cat_id = value_dict.get('cat_id')
                    collect_id = value_dict.get('collect_id')

                    # Câu lệnh INSERT
                    insert_query = """
                    INSERT INTO event_data (event_id, time_stamp, ip, user_agent, resolution, user_id_db, device_id, api_version,
                                        store_id, local_time, show_recommendation, current_url, referrer_url, email_address,
                                        collection, product_id, price, currency, order_id, is_paypal, viewing_product_id,
                                        recommendation, utm_source, utm_medium, options, cart_products, cat_id, collect_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    # Chèn dữ liệu vào PostgreSQL với kiểm tra giá trị
                    data_to_insert = (
                        event_id, time_stamp, ip, user_agent, resolution, user_id_db, device_id, api_version,
                        store_id, local_time, show_recommendation, current_url, referrer_url, email_address,
                        collection, product_id if product_id != "" else None, price if price != "" else None, currency, order_id if order_id != "" else None,
                        is_paypal if is_paypal != "" else None, viewing_product_id if viewing_product_id != "" else None,
                        str(recommendation).lower() if recommendation not in [None, "", "True", "False"] else None,  # Chuyển đổi recommendation thành chuỗi, 
                        utm_source if utm_source != "" else None, utm_medium if utm_medium != "" else None, json.dumps(options), json.dumps(cart_products), cat_id if cat_id != "" else None,
                        collect_id if collect_id != "" else None
                    )
                    # Kiểm tra xem event_id đã tồn tại chưa
                    check_query = "SELECT COUNT(*) FROM event_data WHERE event_id = %s"
                    cursor.execute(check_query, (event_id,))
                    exists = cursor.fetchone()[0] > 0

                    if not exists:
                        cursor.execute(insert_query, data_to_insert)
                        conn.commit()  # Commit ngay sau khi chèn
                    else:
                        logging.info(f"event_id {event_id} đã tồn tại, bỏ qua.")
                    
                except json.JSONDecodeError as e:
                    logging.error(f"Lỗi phân tích JSON: {e}")
                except PostgresError as e:
                    logging.error(f"Lỗi PostgreSQL: {e}")
                    # Thử kết nối lại PostgreSQL
                    conn, cursor = connect_postgres()

            except Exception as e:
                logging.error(f"Lỗi không xác định: {e}")

    except KeyboardInterrupt:
        logging.info("Dừng pull data")
    except Exception as e:
        logging.error(f"Lỗi chính: {e}")
    finally:
        logging.info("Đóng Consumer, Cursor, Connection")
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if consumer:
            consumer.close()

def process_options(options):
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

def process_options_cart_products(options):
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
        }
        options_list.append(option_dict)
    return options_list

def process_cart_products(cart_products, collection):
    if not isinstance(cart_products, list):
        print(f"cart_products không phải là list: {cart_products}")
        return []
    
    return [
        {
            'product_id': str(product.get('product_id')),
            'amount': product.get('amount'),
            'price': process_price(product.get('price')),
            'currency': product.get('currency'),
            'options': process_options_cart_products(product.get('option', []))
        } for product in cart_products
    ]

def process_price(price_str):
    if price_str is None:
        return None
    try:
        price_str = price_str.replace(',', '').replace('.', '')
        price_str = ''.join(filter(str.isdigit, price_str))
        return int(price_str) if price_str else None
    except ValueError:
        print(f"Không thể xử lý giá trị price: {price_str}")
        return None

if __name__ == "__main__":
    main()
