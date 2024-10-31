import configparser
import os

class ConfigReader:
    def __init__(self, config_file='kafka_config.conf'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def get_env_value(self, value):
        """Lấy giá trị từ biến môi trường hoặc giá trị mặc định."""
        if value.startswith('${') and ':' in value:
            env_var, default_value = value.strip('${}').split(':', 1)
            return os.getenv(env_var, default_value)
        else:
            env_var = value.strip('${}')
            return os.getenv(env_var, value)

    def get_config(self, name_section):
        value_dict = {}
        value_dict_postgre = {}
        
        # Duyệt qua tất cả các key-value trong phần KAFKA và đặt vào làm giá trị mặc định
        default_section = 'KAFKA'
        for key, value in self.config[default_section].items():
            env_value = self.get_env_value( value)
            value_dict[key] = env_value  # Thêm giá trị vào từ điển

        # Ghi đè các giá trị từ section khác
        if name_section in self.config:
            for key, value in self.config[name_section].items():
                env_value = self.get_env_value(value)
                value_dict[key] = env_value  # Thêm giá trị vào từ điển

        # Nếu sections là POSTGRES thì xử lý riêng 
        if name_section == "POSTGRES" and name_section in self.config:
            for key, value in self.config[name_section].items():
                value_dict_postgre[key] = self.get_env_value(value)
            return value_dict_postgre
        
        return value_dict


