"""
Given a PDF file, generate a set of topics for it and add to the metadata.
"""
from pdfrw import PdfReader, PdfWriter   
from pprint import pprint

class PdfFileManager():
    def __init__(self, path, model):
        self.reader = PdfReader(path)
        self.path = path
        self.model = model 

    def set_meta_data(self, key, value):
        setattr(self.reader.Info, key, value)   
        PdfWriter(self.path, trailer=self.reader).write()

    def get_meta_data(self):
        return self.reader.Info

    
if __name__ == '__main__':
    pass