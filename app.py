"""
Given a PDF file, generate a set of topics for it and add to the metadata.
"""
import warnings
from io import StringIO
from pprint import pprint

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfrw import PdfReader, PdfWriter, IndirectPdfDict 


class PdfFileManager():
    def __init__(self, path, model):
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

    def parse_abstract(self):
        abs_index = self.raw_data.find('Abstract')
        intro_index = self.raw_data.find('Introduction')
        if (abs_index == -1 or intro_index == -1):
            warnings.warn('Could not locate abstract', UserWarning)
            return None
        else:
            return self.raw_data[abs_index:intro_index]

    
if __name__ == '__main__':
    pass