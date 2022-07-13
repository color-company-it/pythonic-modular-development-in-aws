"""
Configuration handling methods.
"""

import yaml


def load_yaml(filepath: str) -> dict:
    """
    Load yaml config file.
    :param filepath: Local filepath of yaml config.
    :return: Dict object of file data.
    """
    with open(filepath, "r") as _file:
        return yaml.safe_load(_file)
