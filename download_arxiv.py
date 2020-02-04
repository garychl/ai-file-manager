"""
Download the papers from arxiv.

The threading functionality is under development in the branch 'get-token'.
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
from utilities.storage_driver import FileSystemDriver, MongoDrier
from utilities.utils import read_yaml_input, get_logger


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
                try:
                    db_client.insert_many(
                        DB_CONFIG['db_name'], 
                        DB_CONFIG['collection_name'], 
                        doc)
                except TypeError:
                    print('No doc returned.')
                except Exception as e:
                    logger.warn(e)
                    print(e)
                cont_flags.append(cont)  
            cont = all(cont_flags)
            urls, next_tid = scraper.get_urls(next_tid)
    
    logger.info('Done.')
    print('Done.')