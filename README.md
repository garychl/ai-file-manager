# ai-file-manager

An AI file manager to manage the research papers (in the field of machine learning). Usually a research paper invloves serveral concepts and it is hard to organize the papers in the computer file system for at least two reasons:

* You have to read the paper before you can categorize it. This is undesirable when you do not have time or just simply do not want to read any paper.

* It is not easy to have a taxonomy that suits your need every time. Because a research paper is sometimes interdisciplinary or it invovles serveral concepts in a surprising way.

The ideal file manager provides two modes: supervised and unspuervised.

## Use Case

* You find a new research paper online but you do not have time to read at that moment. Over time you may accumulate a lot of unread papers. This app can help tagging the papers you saved locally and add the tags to the metadata.

* With the added tags, you can create script to have an overview of all the papers (e.g. group by topics) or you can assign them to different folders.

## Model

Two NLP models will be used in this project: Doc2vec and Topic modelling.
Doc2vec has been implemented and the model is trained on ~160,000 abstracts of machine learning and deep learning research papers.

## To Use

The project is still under active development.
Yet, the unsupervised clustering is implemented in `app.py` can be used to cluster a set of .pdf research papers.
