"""
Test the download_arxiv.py

The download script has been updated and thus some tests might fail.
"""
import os
import yaml
import unittest
from unittest import mock

import download_arxiv
from download_arxiv import *


class TestScraper(unittest.TestCase):
    def test_url(self):
        CONFIG = read_yaml_input('./config.yaml')
        CRITERIA = CONFIG['dataset']['download']['criteria']
        criterion = CRITERIA[0]
        scraper = ax.Scraper(**criterion)
        self.assertEqual(scraper.filters, criterion['filters'])


class TestFileSystemDriver(unittest.TestCase):
    def test_db(self):
        with mock.patch('download_arxiv.FileSystemDriver.create_db') as mocked_isdir:
            mocked_isdir.return_value = True
            FileSystemDriver('localhost').create_db('par')
            mocked_isdir.assert_called_with('par')

    def test_collection(self):
        with mock.patch('download_arxiv.FileSystemDriver.create_collection') as mocked_isdir:
            mocked_isdir.return_value = True
            FileSystemDriver('localhost').create_collection('par','sub')
            mocked_isdir.create_collection('par','sub')
    
    def test_insert_one(self):
        doc = {'id':1, 'data':['abc']}
        with mock.patch('download_arxiv.FileSystemDriver.insert_one') as mock_exists:
            mock_exists.return_value = True
            FileSystemDriver('localhost').insert_one('par','sub', doc)
            mock_exists.insert_one('par','sub', doc)

    def test_mongo_data(self):
        CONFIG = read_yaml_input('./config.yaml')
        ENV_TYPE = CONFIG['runtime']['env']
        DB_TYPE = 'mongo'
        DB_CONFIG = CONFIG['database'][DB_TYPE][ENV_TYPE]
        CONNECTION_STRING = DB_CONFIG['connection_string']
        db_client = STORAGE_METHODS[DB_TYPE](CONNECTION_STRING)

        collection = db_client.client[DB_CONFIG['db_name']][DB_CONFIG['collection_name']]
        dates = CONFIG['dataset']['download']['criteria'][0]['filters']['created']
        lt_date, gt_date = dates[0], dates[1]
        invalid_docs = collection.find({'created':{'$lt':lt_date, '$gt':gt_date}})
        self.assertEqual(len(list(invalid_docs)), 0)

if __name__ == '__main__':
    unittest.main()