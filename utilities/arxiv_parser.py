""" 
copy from arxivscraper from modifications
"""

from __future__ import print_function
import xml.etree.ElementTree as ET
import datetime
import time
import sys
PYTHON3 = sys.version_info[0] == 3

from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError

OAI = '{http://www.openarchives.org/OAI/2.0/}'
ARXIV = '{http://arxiv.org/OAI/arXiv/}'
BASE = 'http://export.arxiv.org/oai2?verb=ListRecords&'

from arxivscraper.arxivscraper import Scraper, Record

class Scraper2(Scraper):
    def __init__(self, category, date_from=None, date_until=None, t=30, timeout=300, filters={}):
        super().__init__(category, date_from=None, date_until=None, t=30, timeout=300, filters={})

    def scrape(self):
        """ Overwrite the original scrape method for exception handling.
        """
        t0 = time.time()
        tx = time.time()
        elapsed = 0.0
        url = self.url
        print(url)
        ds = []
        k = 1
        while True:

            print('fetching up to ', 1000 * k, 'records...')
            try:
                response = urlopen(url)
            except HTTPError as e:
                if e.code == 503:
                    to = int(e.hdrs.get('retry-after', 30))
                    print('Got 503. Retrying after {0:d} seconds.'.format(self.t))
                    time.sleep(self.t)
                    continue
                else:
                    raise
            k += 1
            xml = response.read()
            root = ET.fromstring(xml)
            records = root.findall(OAI + 'ListRecords/' + OAI + 'record')
            for record in records:
                meta = record.find(OAI + 'metadata').find(ARXIV + 'arXiv')
                try:
                    record = Record(meta).output()
                    if self.append_all:
                        ds.append(record)
                    else:
                        save_record = False
                        for key in self.keys:
                            for word in self.filters[key]:
                                if word.lower() in record[key]:
                                    save_record = True

                        if save_record:
                            ds.append(record)
                except: 
                    print('Cannot fetch the doc: id is {}\n'.format(meta.find(ARXIV + 'id').text.strip().lower().replace('\n', ' ')))

            try:
                token = root.find(OAI + 'ListRecords').find(OAI + 'resumptionToken')
            except:
                return 1
            if token is None or token.text is None:
                break
            else:
                url = BASE + 'resumptionToken=%s' % token.text

            ty = time.time()
            elapsed += (ty-tx)
            if elapsed >= self.timeout:
                break
            else:
                tx = time.time()

        t1 = time.time()
        print('fetching is completed in {0:.1f} seconds.'.format(t1 - t0))
        print ('Total number of records {:d}'.format(len(ds)))
        return ds
