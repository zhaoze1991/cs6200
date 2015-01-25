#!/usr/bin/python
import os, math, sys, urllib, urllib2
import json
import collections
from itertools import islice
from elasticsearch import Elasticsearch
import indexing
es = Elasticsearch()

# ----------------------------------------------------------
# data definition
stopwords = indexing.readFile('./AP_DATA/stoplist.txt')
D = es.count(index = 'ap_dataset')['count'] # D is the total number of documents
K = 100                        # K is the number of documents should be returned
class Document(object):
    """docstring for Document"""
    def __init__(self, the_id, info):
        super(Document, self).__init__()
        self.id = the_id
        self.information = info
# ----------------------------------------------------------
def getInformation(keyword, result):
    df = result['hits']['total']
    output = []
    total = 0.0
    num = 0
    for hit in result['hits']['hits']:
        the_id = hit['_id'].encode('UTF-8')
        document_length = len(hit['_source']['text'].split(' '))
        # print document_length
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
    if num == 0:
        return [output, 0]
    return [output, total / num]
# ----------------------------------------------------------
def termFrequencyInQuery(term, query):
    freq = 0
    for t in query:
        if t == term:
            freq += 1
    return freq
def Okapi_TF(d, avg):
    tf = d.information['tf']
    # TODO double-check if the length is correct
    length = d.information['document_length']
    return tf / (tf + 0.5 + 1.5 * (length / avg))
# ----------------------------------------------------------
def TF_IDF(d, avg):
    return Okapi_TF(d, avg) * math.log10(D / d.information['df'])
# ----------------------------------------------------------
def Okapi_BM25(tfq, d, avg):
    k1 = 1.2
    k2 = 100
    b = 0.75
    tf = d.information['tf']
    df = d.information['df']
    length = d.information['document_length']
    one = math.log10((D + 0.5) / (df + 0.5))
    two = (tf + k1 * tf) / (tf + k1 * (1 - b + b * length / avg))
    three = (tfq + k2 * tfq) / (tfq + k2)
    return one * two * three
def Laplace(d):
    tf = d.information['tf']

# ----------------------------------------------------------
def getPostings(keyword):
    result = es.search(index = 'ap_dataset', doc_type = 'document',
        body = {
        'query' : {
        'match_phrase':{
        'text':{'query':keyword}
        }
        },
        'explain' : 'true',
        'size' : 85000})
    info = getInformation(keyword, result)
    return info
# ----------------------------------------------------------
def sort(data, k):
    # sort the dictionary, and return the first k pairs
   result = sorted(data.items(), key=lambda x:(-x[1],x[0]))
   return list(islice(result, k))
# ----------------------------------------------------------
def search(query, k):
    Okapi_TF_scores = collections.defaultdict(lambda:0.0)
    TF_IDF_scores = collections.defaultdict(lambda:0.0)
    for item in query:
        posting = getPostings(item)
        avg = posting[1]
        # term frequency in query
        freq = termFrequencyInQuery(item, query)
        for doc in posting[0]:
            docno = doc.information['docno']
            Okapi_TF_scores[docno] += Okapi_TF(doc, avg)
            TF_IDF_scores[docno] += TF_IDF(doc, avg)
            # if model == 'TF-IDF':
            #     scores[doc.information['docno']] += TF_IDF(doc, avg)
            #     continue
            # if model == 'Okapi BM25':
            #     scores[doc.information['docno']] += Okapi_BM25(freq, doc, avg)
            #     continue
            # if model == 'Unigram LM with Laplace smoothing':
            #     # TODO
            #     continue
            # if model == 'Unigram LM with Jelinek-Mercer smoothing':
            #     # TODO
            #     continue
    Okapi_TF_scores = sort(Okapi_TF_scores, k)
    TF_IDF_scores = sort(TF_IDF_scores, k)
    return [Okapi_TF_scores, TF_IDF_scores]
    # print scores
searchlist = ['sex']
model = ['Okapi TF', 'TF-IDF', 'Okapi BM25',
         'Unigram LM with Laplace smoothing',
         'Unigram LM with Jelinek-Mercer smoothing']

# ----------------------------------------------------------
def stringOperation(q):
    query = q[q.find('Document'):len(q)-1]
    i = len(query) - 1
    last = len(query) - 1
    while i > 0:
        if query[i].isalpha() == False:
            i -= 1
            continue
        last = i
        break
    query = query[0:last+1]
    query = query.split(' ')
    length = len(query)
    query = query[3:length]
    # skip stop words
    result = []
    for item in query:
        if item == '' or item == ' ':
            continue
        if item +'\n' in stopwords:
            continue
        if item[-1:].isalpha() == False and item[:-1]+'\n' in stopwords:
            continue
        result.append(item)
    return result
# ----------------------------------------------------------
def getQuestion():
    filecontent = indexing.readFile('./AP_DATA/query_desc.51-100.short.txt')
    questions = {}
    for q in filecontent:
        if 'Document' not in q:
            continue
        # get the question number
        num = q.split(' ')[0]
        num = num[0:len(num)-1]
        # get the query
        query = stringOperation(q)
        questions[num]=query
    return questions
# ----------------------------------------------------------
def outputFormat(container, result, temp):
    i = 1
    for r in result:
        temp_ = temp
        temp_ += r[0] + ' ' + str(i) + ' ' + str(r[1]) + ' Exp\n'
        container.append(temp_)
        i += 1
    return container
def writeFile(container, filename):
    f = open(filename, 'w')
    for content in container:
        f.writelines(content)
    f.close()
def doSearch():
    questions = getQuestion()
    Okapi_TF = []
    TF_IDF = []
    # Okapi_BM25 = []
    # Laplace_smoothing = []
    # Jelinek_Mercer = []
    for index in questions:
        temp = ''
        temp += index + ' Q0 '
        result = search(questions[index], K)
        Okapi_TF = outputFormat(Okapi_TF, result[0], temp)
        TF_IDF = outputFormat(TF_IDF, result[1], temp)
    writeFile(Okapi_TF, 'Okpai_TF')
    writeFile(TF_IDF, 'TF_IDF')

def main():
    doSearch()
if __name__ '__main__':
    main()
