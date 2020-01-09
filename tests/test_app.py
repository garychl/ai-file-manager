import unittest
from app import PdfFileManager


class TestMetaData(unittest.TestCase):
    def test_set_metadata(self):
        # set metadaata
        topics = ['topicA', 'topicB']
        pdf1 = PdfFileManager('./data/1502.04390.pdf', '123')
        pdf1.set_meta_data('Topic', topics)

        # reopen the file and check again
        pdf2 = PdfFileManager('./data/1502.04390.pdf', '123')
        print(type(pdf2.reader.Info.Topic))
        self.assertEqual([topic.strip('()') for topic in pdf2.reader.Info.Topic], topics)

if __name__ == '__main__':
    unittest.main()