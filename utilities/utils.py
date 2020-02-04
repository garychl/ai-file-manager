import os
import sys
import logging

import yaml


def read_yaml_input(yaml_input):
    """Reading configuration file."""
    if isinstance(yaml_input, str):
        with open(yaml_input, 'r', newline='') as f:
            try:
                yaml_config = yaml.safe_load(f)
            except yaml.YAMLError as ymlexcp:
                print(ymlexcp)
    elif isinstance(yaml_input, dict):
        yaml_config = yaml_input
    else:
        raise Exception("Only accept dict or path of the .yaml file.")
    return yaml_config


def get_logger(path):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def cd_prj_dir(show=False):
    sub_folder = ['experiments', 'data']
    cur_folder = os.path.basename(os.getcwd())

    if cur_folder in sub_folder:
        os.chdir("../")
        if show: 
            prj_dir = os.getcwd() 
        else:
            prj_dir = 'project dir.'
        print('Changed dir to {}'.format(prj_dir))

    assert 'README.md' in os.listdir(), "Cannot find REAME.md. Make sure current dir is the project dir."  