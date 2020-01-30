import random

import gensim
import gensim; print('gensim version: {}'.format(gensim.__version__))
from gensim.test.utils import get_tmpfile
from gensim.parsing.preprocessing import remove_stopwords, stem_text, strip_punctuation
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from pymongo import MongoClient


class QueryDriver():
    def __init__(self, connection_string):
        self.client = self.init_client(connection_string)
    
    def init_client(self, connection_input):
        raise NotImplementedError
        
    def preprocess_doc(self):
        raise NotImplementedError
        
    def __iter__(self):
        raise NotImplementedError
        

class MongoQueryDriver(QueryDriver):
    def __init__(self, connection_string):
        super().__init__(connection_string)    
    
    def init_client(self, connection_string):
        return MongoClient(connection_string)
    
    def preprocess(self, doc):
        doc = remove_stopwords(doc)
        doc = strip_punctuation(doc)
        tokens = gensim.utils.simple_preprocess(doc, deacc=False, min_len=2, max_len=20)
        return tokens
    
    def get_stream_doc(self, db_name, collection_name, limits=1e10, tokens_only=False):
        print('Streaming data randomly with limit: {}'.format(limits))
        limit_count = 1
        collection_obj = self.client[db_name][collection_name]
        docs = list(collection_obj.find({}))
        random.shuffle(docs)
        for doc in docs:
            if limit_count % 1000 == 0:
                print('limit_count: {}'.format(limit_count))
            if limit_count > limits:
                break
            else:
                limit_count += 1
                tag = str(doc['_id'])
                abstract = doc['abstract']
                title = doc['title']
                tokens = self.preprocess(abstract+title)
                if tokens_only:
                    yield tokens
                else:
                    yield TaggedDocument(tokens, [tag])            
          