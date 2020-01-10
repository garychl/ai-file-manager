"""
Download the papers from arxiv.

Note: 
    -   Since an off-the -shelf arxiv xml parser is used, using mongobd might be an overkill. 
    -   Since the data size is still manageblae even query 5 years data at once,
    the data are fetched, parsed and inserted at once. 
        If data size is large, the script need to modify to fetch less data, with the cost 
    increased network traffic overheads.
"""
import yaml
import utils.arxiv_parser as ax
from pymongo import MongoClient
from pprint import pprint


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
    

class Downloader():
    def __init__(self, config_input='./config.yaml'):
        self.config = read_yaml_input(config_input)
        self.criteria = self.config['dataset']['download']['criteria']

    def download(self):
        documents_list = []
        for criterion in self.criteria:
            print('Fetching the data with the criterion:\n{}'.format(criterion))
            scraper = ax.Scraper2(**criterion)
            documents = scraper.scrape()
            print('Completed downloading. Downloaded {} documents.'.format(len(documents)))
            documents_list.append(documents)
        return documents_list


class StorageDriver():
    def __init__(self, config_input='./config.yaml'):
        self.config = read_yaml_input(config_input)
        self.runtime_config = self.config['runtime']
        self.init_client()

    def init_client(self):
        raise NotImplementedError

    def create_db(self):
        raise NotImplementedError

    def create_collection(self):
        raise NotImplementedError

    def insert_many(self, documents_list):
        raise NotImplementedError


class MongoDrier(StorageDriver):
    def __init__(self, config_input='./config.yaml'):
        super().__init__(config_input)

    def init_client(self):
        self.db_config = self.config['database']['mongo'][self.runtime_config['env']]
        self.client = MongoClient(self.db_config['connection_string'])

    def create_db(self):
        db_name = self.db_config['db_name']
        self.db = getattr(self.client, db_name)

    def create_collection(self):
        self.create_db()
        collection_name = self.db_config['collection_name']
        self.collection = getattr(self.db, collection_name)

    def insert_many(self, documents_list):
        self.create_collection()
        print('Inserting into mongodb...')
        for documents in documents_list:
            self.collection.insert_many(documents)
        print('Finished insertion.')   


storage_methods = {'mongo': MongoDrier, 'mysql':None, 'file_system': None}

if __name__ == '__main__':
    db = read_yaml_input('./config.yaml')['runtime']['db']
    db_client = storage_methods[db]()
    documents_list = Downloader().download()
    db_client.insert_many(documents_list)
    print('Done.')