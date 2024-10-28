# kafka-postgres

## Mục lục 
-[Tổng quan](tổng-quan)

-[Công nghệ sử dụng](công-nghệ-sử-dụng)

-[Luồng dữ liệu](luồng-dữ-liệu)

-[Cấu trúc dự án](cấu-trúc-dự-án)

-[Hình ảnh](hình-ảnh)

## Tổng quan
Dự án này sử dụng python để làm việc với kafka cụ thể là tạo một consumer để xử lý việc đọc dữ liệu từ một topic và lữu trữ nó vào database (***Postgresql***)

## Công nghệ sử dụng
+ Docker
+ Apache Kafka
+ PostgreSQL
+ Python
+ Adminer (công cụ quản lý cơ sở dữ liệu dựa trên web)

## Luồng dữ liệu
để đọc dữ liệu từ Kafka và lưu trữ vào PostgreSQL trong dự án này, cần thực hiện các bước chính sau:

1. Định nghĩa hàm connect_postgres() để kết nối đến cơ sở dữ liệu PostgreSQL.
2. Trong hàm main():

  Khởi tạo một Consumer Kafka và đăng ký topic cần đọc.
  Kết nối đến PostgreSQL sử dụng hàm connect_postgres().
  Trong vòng lặp vô hạn:

  Đọc dữ liệu từ Kafka.
  Xử lý dữ liệu (giải mã nếu cần).
  Thực hiện câu lệnh SQL để lưu dữ liệu vào PostgreSQL.
  Xử lý các lỗi có thể xảy ra.


  Đóng kết nối Kafka và PostgreSQL khi kết thúc.


  Lưu thông tin cấu hình kết nối PostgreSQL trong file postgres_config.py.

Bằng cách triển khai các bước này, ứng dụng sẽ đọc dữ liệu từ Kafka và lưu trữ vào cơ sở dữ liệu PostgreSQL.
## Cấu trúc dự án
<pre>
<code>
project/
│
├── <span style="color: #4CAF50;">consumer.py</span>   # Script chính để khởi tạo một consumer
├── <span style="color: #4CAF50;">consumer_2.py</span>   # Script phụ để chia tài nguyên khi pull về 
├── <span style="color: #4CAF50;">consumer_3.py</span>   # Script phụ để chia tài nguyên khi pull về 
</code>
</pre>
  
## Hình ảnh
![image](https://github.com/user-attachments/assets/4004c0bd-3c2a-47db-9c2a-be0cf30e9e82)
![image](https://github.com/user-attachments/assets/e13f0bf0-a9e9-4310-9068-47f8e150d973)
![image](https://github.com/user-attachments/assets/5a202ddb-2f3c-4049-9877-a737ce5e9938)


