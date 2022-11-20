import configparser


def read_ini(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config
