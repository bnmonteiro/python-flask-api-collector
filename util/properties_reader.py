import yaml


CONFIG_FILE_PATH = '/root/app-collector/config.yaml'


def read_properties():
    with open(CONFIG_FILE_PATH, 'r') as yaml_file:
        properties = yaml.safe_load(yaml_file)
    return properties


def property_value(section_name, property_key):
    properties = read_properties()
    return properties[section_name][property_key]

