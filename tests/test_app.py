import unittest
from app import PdfFileManager


class MyTest(unittest.TestCase):
    def test(self):
        # set metadaata
        topics = ['topicA', 'topicB']
        pdf1 = PdfFileManager('./data/1502.04390.pdf', '123')
        pdf1.set_meta_data('Topic', topics)

        # reopen the file and check again
        pdf2 = PdfFileManager('./data/1502.04390.pdf', '123')
        self.assertEqual(pdf2.reader.Info.Topic, ['({})'.format(topic) for topic in topics])

if __name__ == '__main__':
    unittest.main()