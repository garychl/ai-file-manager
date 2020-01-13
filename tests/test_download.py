"""
Test the download_arxiv.py
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
        scraper = ax.Scraper2(**criterion)
        self.assertEqual(scraper.f, criterion['date_from'])
        self.assertEqual(scraper.u, criterion['date_until'])
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

if __name__ == '__main__':
    unittest.main()