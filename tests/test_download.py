import unittest
from unittest import mock
import download_arxiv
from download_arxiv import FileSystemDriver
import os


class TestFileSystemDriver(unittest.TestCase):
    def test_db(self):
        with mock.patch('download_arxiv.FileSystemDriver.create_db') as mocked_isdir:
            mocked_isdir.return_value = True
            FileSystemDriver.create_db('par')
            mocked_isdir.assert_called_with('par')

    def test_collection(self):
        with mock.patch('download_arxiv.FileSystemDriver.create_collection') as mocked_isdir:
            mocked_isdir.return_value = True
            FileSystemDriver.create_collection('par','sub')
            mocked_isdir.create_collection('par','sub')
    
    def test_insert_one(self):
        doc = {'id':1, 'data':['abc']}
        with mock.patch('download_arxiv.FileSystemDriver.insert_one') as mock_exists:
            mock_exists.return_value = True
            FileSystemDriver.insert_one('par','sub', doc)
            mock_exists.insert_one('par','sub', doc)

if __name__ == '__main__':
    unittest.main()