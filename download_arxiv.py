"""
Download the papers from arxiv.

Note: 
    -   Since an off-the -shelf arxiv xml parser is used, using mongobd might be an overkill. 
    -   Since the data size is still small even querying 5 years data,
    the data are fetched into memory, parsed and inserted at once. 
        If data size is large, the script need to modify to fetch less data, with the cost 
    increased network traffic overheads.
"""
import os
import yaml
import utils.arxiv_parser as ax
from pymongo import MongoClient
from pprint import pprint
import pickle

def read_yaml_input(input):
    if type(input) == str:
        with open(input, 'r', newline='') as f:
            try:
                config = yaml.safe_load(f)
            except yaml.YAMLError as ymlexcp:
                print(ymlexcp)
    elif type(input) == dict:
        config = input
    else:
        raise Exception("Only accept dict or path of the .yaml file.")
    return config


def correct_file_name(file_name):
    return file_name.replace("/", "_")

def scrape_arxiv(criterion):
    if type(criterion) != dict:
        raise Exception("criterion is a dict.")
    print('Fetching the data with the criterion:\n{}'.format(criterion))
    scraper = ax.Scraper2(**criterion)
    documents = scraper.scrape()    # list of dict
    print('Completed downloading. Downloaded {} documents.'.format(len(documents)))
    return documents


class StorageDriver():
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

    def insert_one(self, db_name, collection_name, document, file_name=None, format='.pkl'):
        try:
            file_name = document['id']
        except KeyError:
            assert file_name != None, "Please provide a file name for doc:/n{}".format(document)
            file_name = file_name

        if format == '.pkl':
            file_name = correct_file_name(file_name) + '.pkl'
            collection_dir = self.create_collection(db_name, collection_name)
            with open(os.path.join(collection_dir, file_name), 'wb') as f:
                pickle.dump(document, f, pickle.HIGHEST_PROTOCOL)
        else:
            raise "{} is not a currently supported format".format(format)

    def insert_many(self, db_name, collection_name, documents, files_name=None, format='.pkl'):
        print('Writing into file system ...')
        if files_name is not None:
            for document, file_name in zip(documents, files_name):
                self.insert_one(db_name, collection_name, document, file_name)
        else:
            for document in documents:
                self.insert_one(db_name, collection_name, document)
        
        print('Finished Download.')          


class MongoDrier(StorageDriver):
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


storage_methods = {'mongo': MongoDrier, 'mysql':None, 'file_system': FileSystemDriver}

if __name__ == '__main__':
    config = read_yaml_input('./config.yaml')
    # runtime configuration
    run_time_config = config['runtime']
    db_type = run_time_config['db']
    env_type = run_time_config['env']
    # db configuartion at runtime
    db_config = config['database'][db_type][env_type]
    
    # init db
    connection_string = db_config['connection_string']
    db_client = storage_methods[db_type](connection_string)

    # download AI papers; here we put the papers in the same 
    # collection even they may comes from different categories,
    # for example computer science and statistics
    criteria = config['dataset']['download']['criteria']
    for criterion in criteria:
        documents = scrape_arxiv(criterion)
        db_client.insert_many(db_config['db_name'], db_config['collection_name'], documents)
    print('Done')
