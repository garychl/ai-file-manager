import warnings
import os
import glob
from io import StringIO
from pprint import pprint

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfrw import PdfReader, PdfWriter, IndirectPdfDict 
from sklearn.cluster import KMeans
from gensim.utils import simple_preprocess
from gensim.models.doc2vec import Doc2Vec


class PdfFileManager():
    def __init__(self, path):
        self.path = path
        output_string = StringIO()
        with open(path, 'rb') as in_file:
            parser = PDFParser(in_file)
            doc = PDFDocument(parser)
            rsrcmgr = PDFResourceManager()
            device = TextConverter(rsrcmgr, output_string, laparams=LAParams())
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            for page in PDFPage.create_pages(doc):
                interpreter.process_page(page)
            self.raw_data = output_string.getvalue()
        output_string.close()


    def set_meta_data(self, key, value):
        # using code from other module - see if it can improved later
        reader = PdfReader(self.path)
        assert "/"+key not in reader.Info, \
               "{} already existed in metadata".format(key)
        setattr(reader.Info, key, value)   
        PdfWriter(self.path, trailer=reader).write()
        
        
    def get_meta_data(self):
        with open(self.path, 'rb') as in_file:
            parser = PDFParser(in_file)
            doc = PDFDocument(parser)
            meta_data = doc.info
            if len(meta_data) > 1:
                warnings.warn('len of metadata >1', UserWarning)
        return meta_data

    def process_text(self, text):
        processed_text = simple_preprocess(text)
        return processed_text

    def parse_abstract(self):
        processed_text = self.process_text(self.raw_data)
        try:
            abs_index = processed_text.index('abstract')
            intro_index = processed_text.index('introduction')
            return processed_text[abs_index:intro_index]
        except:
            warnings.warn('Could not locate abstract', UserWarning)
            return None
            