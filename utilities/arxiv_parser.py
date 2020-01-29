""" 
copy from arxivscraper from modifications
"""

from __future__ import print_function
import xml.etree.ElementTree as ET
import datetime
from dateutil.parser import parse
import time
import random
import sys
PYTHON3 = sys.version_info[0] == 3
import concurrent.futures

import numpy as np 
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError

OAI = '{http://www.openarchives.org/OAI/2.0/}'
ARXIV = '{http://arxiv.org/OAI/arXiv/}'
BASE = 'http://export.arxiv.org/oai2?verb=ListRecords&'

# from arxivscraper.arxivscraper import Scraper, Record


class Record(object):
    """
    A class to hold a single record from ArXiv
    Each records contains the following properties:

    object should be of xml.etree.ElementTree.Element.
    """

    def __init__(self, xml_record):
        """if not isinstance(object,ET.Element):
        raise TypeError("")"""
        self.xml = xml_record
        self.id = self._get_text(ARXIV, 'id')
        self.url = 'https://arxiv.org/abs/' + self.id
        self.title = self._get_text(ARXIV, 'title')
        self.abstract = self._get_text(ARXIV, 'abstract')
        self.cats = self._get_text(ARXIV, 'categories')
        self.created = self._get_text(ARXIV, 'created')
        self.updated = self._get_text(ARXIV, 'updated')
        self.doi = self._get_text(ARXIV, 'doi')
        self.authors = self._get_authors()
        self.affiliation = self._get_affiliation()

    def _get_text(self, namespace, tag):
        """Extracts text from an xml field"""
        try:
            return self.xml.find(namespace + tag).text.strip().lower().replace('\n', ' ')
        except:
            return ''

    def _get_authors(self):
        authors_xml = self.xml.findall(ARXIV + 'authors/' + ARXIV + 'author')
        last_names = [author.find(ARXIV + 'keyname').text.lower() for author in authors_xml]
        first_names = [author.find(ARXIV + 'forenames').text.lower() for author in authors_xml]
        full_names = [a+' '+b for a,b in zip(first_names, last_names)]
        return full_names

    def _get_affiliation(self):
        authors = self.xml.findall(ARXIV + 'authors/' + ARXIV + 'author')
        try:
            affiliation = [author.find(ARXIV + 'affiliation').text.lower() for author in authors]
            return affiliation
        except:
            return []

    def output(self):
        d = {
            'title': self.title,
            'id': self.id,
            'abstract': self.abstract,
            'categories': self.cats,
            'doi': self.doi,
            'created': self.created,
            'updated': self.updated,
            'authors': self.authors,
            'affiliation': self.affiliation,
            'url': self.url
             }
        return d


class Scraper(object):
    """
    A class to hold info about attributes of scraping,
    such as date range, categories, and number of returned
    records. If `from` is not provided, the first day of
    the current month will be used. If `until` is not provided,
    the current day will be used.

    Paramters
    ---------
    category: str
        The category of scraped records
    data_from: str
        starting date in format 'YYYY-MM-DD'. Updated eprints are included even if
        they were created outside of the given date range. Default: first day of current month.
    date_until: str
        final date in format 'YYYY-MM-DD'. Updated eprints are included even if
        they were created outside of the given date range. Default: today.
    t: int
        Waiting time between subsequent calls to API, triggred by Error 503.
    timeout: int
        Timeout in seconds after which the scraping stops. Default: 300s
    filter: dictionary
        A dictionary where keys are used to limit the saved results. Possible keys:
        subcats, author, title, abstract. See the example, below.

    Example:
    Returning all eprints from

    ```
        import arxivscraper.arxivscraper as ax
        scraper = ax.Scraper(category='stat',date_from='2017-12-23',date_until='2017-12-25',t=10,
                 filters={'affiliation':['facebook'],'abstract':['learning']})
        output = scraper.scrape()
    ```
    """

    def __init__(self, category, workers=5, date_from=None, date_until=None, t=30, timeout=300, filters={}):
        self.cat = str(category)
        self.workers = workers
        self.t = t
        self.timeout = timeout
        # DateToday = datetime.date.today()
        # if test:
        #     if date_from is None:
        #         self.f = str(DateToday.replace(day=1))
        #     else:
        #         self.f = date_from
        #     if date_until is None:
        #         self.u = str(DateToday)
        #     else:
        #         self.u = date_until
        #     self.url = BASE + 'from=' + self.f + '&until=' + self.u + '&metadataPrefix=arXiv&set=%s' % self.cat
        # else:
        #     self.url = BASE + '&metadataPrefix=arXiv&set=%s' % self.cat
        self.url = BASE + 'metadataPrefix=arXiv&set=%s' % self.cat
        self.token = self.get_token(self.url)
        self.filters = filters
        if not self.filters:
            self.append_all = True
        else:
            self.append_all = False
            self.keys = filters.keys()

    def _get_dates(self, values):
        end = None
        if isinstance(values, list):
            if len(values) == 1:
                start = values[0]
            elif len(values) == 2:
                start = values[0]
                end = values[1]
            else:
                raise "Please pass only two dates for created as the search range."
        else:
            raise "Only accept list for created filter"
        return start, end

    def _in_date_range(self, start, end, target):
        satisfy = False
        start = parse(start)
        target = parse(target)

        if end is not None:
            end = parse(end)
            if start <= target <= end:
                satisfy = True
        else:
            if start == target:
                satisfy = True
        return satisfy

    def get_token(self, url):
        response = urlopen(url)
        xml = response.read()
        root = ET.fromstring(xml)
        resumptionToken = root.find(OAI + 'ListRecords').find(OAI + 'resumptionToken').text
        api_token = resumptionToken[:resumptionToken.find('|')]
        return api_token

    def get_urls(self, start=1):
        tids = list(range(start, (start+self.workers*1000), 1000))
        urls = [BASE+'resumptionToken='+self.token+'|'+str(tid) for tid in tids]
        return urls

    def test_error(self):
        url = BASE+'resumptionToken='+self.token+'|'+str(800000)
        response = urlopen(url)
        print(response)
        xml = response.read()
        print('xml',xml)
        root = ET.fromstring(xml)
        print('root',root)
        # below resumptionToken is None if there is no returned result
        resumptionToken = root.find(
                OAI + 'ListRecords').find(
                OAI + 'resumptionToken').text
        print('resumptionToken', resumptionToken)

    def scrape_one(self, url):
        ds = []
        print('Scraping url: {}'.format(url))
        tid = url[url.find('|')+1:]
        
        t = random.randint(30,60)   # start the threads at different time
        success = False
        while not success:
            try:
                response = urlopen(url)
                success = True
            except HTTPError as e:
                if e.code == 503:
                    print('Got 503. Retrying after {0:d} seconds.'.format(t))
                    time.sleep(t)
                    continue
                else:
                    raise
        
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
                    # save only when the record satisfy all the filter requirements
                    satisfy_requirement = []
                    for key, values in self.filters.items():
                        if key == 'created':
                            start, end = self._get_dates(values)
                            satisfy = self._in_date_range(start, end, record[key])
                        else:
                            # satisfy if any of the value is found
                            satisfy = False
                            for word in values:
                                if word.lower() in record[key]:
                                    satisfy = True
                        satisfy_requirement.append(satisfy)
                    
                    if all(satisfy_requirement):
                        ds.append(record)
            except: 
                # pass
                print('tid: {} - Cannot fetch the doc: id is {}\n'.format(
                    tid, meta.find(ARXIV + 'id').text.strip().lower().replace('\n', ' ')))

        print ('Total number of records {:d}'.format(len(ds)))
        try:
            resumptionToken = root.find(OAI + 'ListRecords').find(OAI + 'resumptionToken').text
        except:
            resumptionToken = None
        cont = True if resumptionToken else False
        return ds, cont

    def scrape_many(self, urls):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(self.scrape_one, urls)
        return results
