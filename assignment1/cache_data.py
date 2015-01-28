#!/usr/local/bin/python
import urllib, urllib2
import re
import sys
from elasticsearch import Elasticsearch
import re

es = Elasticsearch()
D = es.count(index = 'ap_dataset', doc_type='document')['count']
all_item = {}
# document_length = {}
V = 0
average = 0.0
def cache():
    _id = 0
    avg = 0.0
    n = 0
    container = []
    while _id < D :
        # print _id
        term = es.termvector(index = 'ap_dataset', doc_type='document', id = _id)
        if 'text' not in term['term_vectors']:
            _id += 1
            continue
        document_length = {}
        string = str(term)
        patter = re.compile("'term_freq': (\d+)")
        num = patter.findall(string)
        total = sum(map(lambda s: int(s), num))
        term = term['term_vectors']['text']['terms']
        term = term.keys()
        for t in term:
            t = t.encode('UTF-8')
            if t in all_item:
                continue
            all_item[t] = 1
        text = es.search(index = 'ap_dataset', doc_type = 'document',
            body = {
            'query' : {
            'match_phrase':{
            '_id':{'query':_id}
            }}})
        n += 1
        text = text['hits']['hits'][0]['_source']
        docno = text['docno'].encode('UTF-8')
        document_length[docno] = total
        container.append(document_length)
        avg += total
        _id += 1
    global average
    average = avg / n
    global V
    V =len(all_item)
    save = open('cache2','w')
    save.writelines(str(V) +'\n')
    save.writelines(str(average) + '\n')
    for document_length in container:
        for key in document_length:
            string = key + ' ' + str(document_length[key])+'\n'
            save.writelines(string)
    save.close()
    # print document_length
# cache()
