import configparser
import os

class ConfigReader:
    def __init__(self, config_file='kafka_config.conf'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)


    def get_config(self, name_section):
        value_dict = {}
        # Ghi các giá trị từ section khác
        if name_section in self.config:
            for key, value in self.config[name_section].items():
                """Lấy giá trị từ biến môi trường hoặc giá trị mặc định."""
                if value.startswith('${') and ':' in value:
                    env_var, default_value = value.strip('${}').split(':', 1)
                    value_dict[key] = os.getenv(env_var, default_value)
                else:
                    env_var = value.strip('${}')
                    value_dict[key] =  os.getenv(env_var, value)
        return value_dict

if __name__ == "__main__":
    config_reader = ConfigReader()
    print(config_reader.get_config("KAFKA_CONSUMER_MAIN"))


