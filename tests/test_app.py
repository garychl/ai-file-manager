import unittest
from app import PdfFileManager


class MyTest(unittest.TestCase):
    def test(self):
        pdf = PdfFileManager('./data/1502.04390.pdf', '123')
        pdf.set_meta_data('Topic', ['topicA, topicB'])
        self.assertEqual(pdf.reader.Info.Topic, ['topicA, topicB'])

if __name__ == '__main__':
    unittest.main()