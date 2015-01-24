#!/usr/bin/python
import os, math
import json
import collections
from itertools import islice
from elasticsearch import Elasticsearch
es = Elasticsearch()
D = es.count(index = 'ap_dataset')['count']
class myDictionary(dict):
    def __get__item(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

class Document(object):
    """docstring for Document"""
    def __init__(self, the_id, info):
        super(Document, self).__init__()
        self.id = the_id
        self.information = info

def getInformation(keyword, result):
    df = result['hits']['total']
    output = []
    total = 0.0
    num = 0
    for hit in result['hits']['hits']:
        the_id = hit['_id'].encode('UTF-8')
        document_length = len(hit['_source']['text'])
        total += document_length
        num += 1
        docno = hit['_source']['docno'].encode('UTF-8')
        tf = hit['_explanation']['details'][0]['details'][0]['details'][0]['value']
        temp = {
        'document_length':document_length,
        'docno': docno,
        'tf': tf,
        'df': df
        }
        d = Document(the_id, temp)
        # print d.information
        output.append(d)
    # posting and the average length
    return [output, total / num]
def Okapi_TF(w, d, avg):
    tf = d.information['tf']
    length = d.information['document_length']
    return tf / (tf + 0.5 + 1.5 * (length / avg))
def TF_IDF(w, d, avg):
    return Okapi_TF(w, d, avg) * math.log10(D / d.information['df'])

def getPostings(keyword):
    result = es.search(index = 'ap_dataset', doc_type = 'document',
        body = {
        'query' : {
        'match_phrase':{
        'text':{'query':keyword}
        }
        },
        'explain' : 'true',
        # sort the document by docno
        # 'sort' : [{'docno':{'order':'asc'}}],
        'size' : 85000})
    info = getInformation(keyword, result)
    return info

def sort(data, k):
    # sort the dictionary, and return the first k pairs
   result = sorted(data.items(), key=lambda x:(-x[1],x[0]))
   return list(islice(result, k))

def search(query, k):
    scores = collections.defaultdict(lambda:0.0)
    for item in query:
        posting = getPostings(item)
        avg = posting[1]
        for doc in posting[0]:
            s = Okapi_TF(item, doc, avg)
            scores[doc.information['docno']] += s
    scores = sort(scores, k)
    for key in scores:
        print key
searchlist = ['sex']
search(searchlist, 100)
