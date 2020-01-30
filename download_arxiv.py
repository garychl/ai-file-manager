"""
Download the papers from arxiv.
"""
import os
import time
import pickle
import logging
from datetime import datetime
from pprint import pprint

import yaml
from pymongo import MongoClient

import utilities.arxiv_parser as ax


def read_yaml_input(yaml_input):
    """Reading configuration file."""
    if isinstance(yaml_input, str):
        with open(yaml_input, 'r', newline='') as file:
            try:
                yaml_config = yaml.safe_load(file)
            except yaml.YAMLError as ymlexcp:
                print(ymlexcp)
    elif isinstance(yaml_input, dict):
        yaml_config = yaml_input
    else:
        raise Exception("Only accept dict or path of the .yaml file.")
    return yaml_config

def correct_file_name(file_name):
    """To ensure file name will not cause error."""
    return file_name.replace("/", "_")

def get_logger(path):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


class StorageDriver():
    """Base class for stroage client."""
    def __init__(self, connection_string):
        self.client = self.init_client(connection_string)

    def init_client(self, connection_input):
        raise NotImplementedError

    def create_db(self, db_name):
        raise NotImplementedError

    def create_collection(self, db_name, collection_name):
        raise NotImplementedError

    def insert_many(self, db_name, collection_name, documents):
        raise NotImplementedError


class FileSystemDriver(StorageDriver):
    """File system storage client."""
    def __init__(self, connection_string):
        super().__init__(connection_string)

    def init_client(self, connection_string):
        pass

    def create_db(self, db_name):
        db_dir = os.path.join('./data', db_name)
        if not os.path.isdir(db_dir):
            print('Creating a new directory: {}'.format(db_dir))
            os.mkdir(db_dir)
        return db_dir

    def create_collection(self, db_name, collection_name):
        db_dir = self.create_db(db_name)
        collection_dir = os.path.join(db_dir, collection_name)
        if not os.path.isdir(collection_dir):
            print('Creating a new directory: {}'.format(collection_dir))
            os.mkdir(collection_dir)
        return collection_dir

    def insert_one(self, db_name, collection_name, document, file_name=None, out_format='.pkl'):
        try:
            file_name = document['id']
        except KeyError:
            assert file_name is not None, "Please provide a file name for doc:/n{}".format(document)
            file_name = file_name

        if out_format == '.pkl':
            file_name = correct_file_name(file_name) + '.pkl'
            collection_dir = self.create_collection(db_name, collection_name)
            with open(os.path.join(collection_dir, file_name), 'wb') as f:
                pickle.dump(document, f, pickle.HIGHEST_PROTOCOL)
        else:
            raise "{} is not a currently supported format".format(out_format)

    def insert_many(self, db_name, collection_name, documents, files_name=None, out_format='.pkl'):
        print('Writing into file system ...')
        if files_name is not None:
            for document, file_name in zip(documents, files_name):
                self.insert_one(db_name, collection_name, document, file_name)
        else:
            for document in documents:
                self.insert_one(db_name, collection_name, document)
        print('Finished Download.')


class MongoDrier(StorageDriver):
    """MongoDB storage client."""
    def __init__(self, connection_string):
        super().__init__(connection_string)

    def init_client(self, connection_string):
        return MongoClient(connection_string)

    def create_db(self, db_name):
        return getattr(self.client, db_name)

    def create_collection(self, db_name, collection_name):
        db = self.create_db(db_name)
        return getattr(db, collection_name)

    def insert_many(self, db_name, collection_name, documents):
        collection = self.create_collection(db_name, collection_name)
        print('Inserting into mongodb...')
        collection.insert_many(documents)
        print('Finished insertion.')

STORAGE_METHODS = {'mongo': MongoDrier,
                   'mysql': None,
                   'file_system': FileSystemDriver}


if __name__ == '__main__':
    logger = get_logger('./log/arxiv/download.log')
    logger.info('Start running at: {}'.format(
        datetime.today().strftime('%Y-%m-%d-%H:%M:%S')))

    CONFIG = read_yaml_input('./config.yaml')
    RUN_TIME_CONFIG = CONFIG['runtime']
    logger.info('Runtime config: {}'.format(RUN_TIME_CONFIG))
    DB_TYPE = RUN_TIME_CONFIG['db']
    ENV_TYPE = RUN_TIME_CONFIG['env']
    DB_CONFIG = CONFIG['database'][DB_TYPE][ENV_TYPE]
    logger.info('DB config: {}'.format(DB_CONFIG))

    # init db
    CONNECTION_STRING = DB_CONFIG['connection_string']
    db_client = STORAGE_METHODS[DB_TYPE](CONNECTION_STRING)

    # download AI papers; here we put the papers in the same \
    # collection even they may comes from different categories, \
    # for example computer science and statistics \
    CRITERIA = CONFIG['dataset']['download']['criteria']
    for criterion in CRITERIA:
        print('Scraping criterion: {}'.format(criterion))
        logger.info('criterion: {}'.format(criterion))
        scraper = ax.Scraper(**criterion)
        urls, next_tid = scraper.get_urls()
        
        cont = True
        while cont:
            cont_flags = []
            results = scraper.scrape_many(urls)
            for result in results:
                doc, cont = result
                print('len of doc: {}, cont: {}'.format(len(doc), cont))
                try:
                    db_client.insert_many(
                        DB_CONFIG['db_name'], 
                        DB_CONFIG['collection_name'], 
                        doc)
                except TypeError:
                    print('No doc returned.')
                except Exception as e:
                    print(e)
                cont_flags.append(cont)  
            cont = all(cont_flags)
            print('all(cont_flags):', cont)
            urls, next_tid = scraper.get_urls(next_tid)
    
    logger.info('Done.')
    print('Done.')