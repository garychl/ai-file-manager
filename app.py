"""
Given a PDF file, generate a set of topics for it and add to the metadata.
"""
import os
import glob
from pprint import pprint

from sklearn.cluster import KMeans
from gensim.utils import simple_preprocess
from gensim.models.doc2vec import Doc2Vec

from utilities.utils import read_yaml_input
from pdf_manager import PdfFileManager


class AppManager():
    def __init__(self, papers_dir, doc2vec_model=None):
        self.papers_dir = papers_dir
        self.docs_path = glob.glob(papers_dir+"*.pdf")
        self.doc2vec_model = doc2vec_model

    def get_docs_embeddings(self):
        docs_embeddings_dic = {}
        for doc_path in self.docs_path:
            print('parsing {}'.format(doc_path))
            processed_doc = PdfFileManager(doc_path).parse_abstract()
            if processed_doc:
                print('Computing embeddings ...')
                processed_doc_embed = self.doc2vec_model.infer_vector(processed_doc)
                docs_embeddings_dic[doc_path] = processed_doc_embed
            else:
                print('No parsed contennt for {}'.format(doc_path))
        return docs_embeddings_dic

    def cluster_papers_kmeans(self, n_clusters=4, random_state=0):
        docs_embeddings_dic = self.get_docs_embeddings()
        print('{} documents will be clustered into {}'.format(len(docs_embeddings_dic), n_clusters))
        kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)
        cluster_results = kmeans.fit_predict(list(docs_embeddings_dic.values()))
        docs_clusters = {doc: clus for doc, clus in zip(list(docs_embeddings_dic.keys()), cluster_results)}
        return docs_clusters

    def auto_cluster_papers(self):
        pass

    def mv_to_folders_by_clusters(self):
        pass

    
if __name__ == '__main__':
    CONFIG = read_yaml_input('config.yaml')
    ENV_TYPE = CONFIG['runtime']['env']
    MODEL_NAME = CONFIG['model']['saved'][ENV_TYPE]
    MODEL_PATH = os.path.join('model', MODEL_NAME)
    if os.path.isfile(MODEL_PATH):
        print('Loading saved model...')
        doc2vec_model = Doc2Vec.load(MODEL_PATH)
    else:
        raise "Cannot locate the model."

    app_mng = AppManager('../paper/', doc2vec_model)
    docs_cluster = app_mng.cluster_papers_kmeans(4)
    pprint(docs_cluster)
