#!/usr/local/bin/python
import os, math, sys, urllib, urllib2
import json
import collections
from itertools import islice
from elasticsearch import Elasticsearch
import indexing, cache_data
import re
es = Elasticsearch()

# ----------------------------------------------------------
# data definition

recentsearch = {}
D = es.count(index = 'ap_dataset')['count'] # D is the total number of documents
K = 100                        # K is the number of documents should be returned
V = 10000000
average = 200.0
document_length = {}
class Document(object):
    """docstring for Document"""
    def __init__(self, the_id, info):
        super(Document, self).__init__()
        self.id = the_id
        self.information = info
# ----------------------------------------------------------
def getInformation(keyword, result):
    # this function will return df, tf etc. and return all the needed
    # information in a list
    df = result['hits']['total']
    output = []
    total = 0.0
    num = 0
    for hit in result['hits']['hits']:
        the_id = hit['_id'].encode('UTF-8')
        docno = hit['_source']['docno'].encode('UTF-8')
        try:
            TARGET = re.compile('.*termFreq=(.*?)\'.*')
            details = str(hit['_explanation']['details'])
            tf = float(TARGET.match(details).group(1))
        except AttributeError:
            TARGET = re.compile('.*phraseFreq=(.*?)\'.*')
            details = str(hit['_explanation']['details'])
            tf = float(TARGET.match(details).group(1))
        temp = {
            'document_length': document_length[docno],
            'docno': docno,
            'tf': tf,
            'df': df
        }
        if tf  == -1:
            print keyword
            sys.exit(-1)
        d = Document(the_id, temp)
        output.append(d)
    return output
# ----------------------------------------------------------
def termFrequencyInQuery(term, query):
    freq = 0
    for t in query:
        if t == term:
            freq += 1
    return freq
# ----------------------------------------------------------
def Okapi_TF(d):
    global average
    tf = d.information['tf']
    length = d.information['document_length']
    return tf / (tf + 0.5 + 1.5 * (length / average))
# ----------------------------------------------------------
def TF_IDF(d):
    df = d.information['df']
    return Okapi_TF(d) * math.log10(D / df)
# ----------------------------------------------------------
def Okapi_BM25(tfq, d):
    k1 = 1.2
    k2 = 100.0
    b = 0.75
    global average
    tf = d.information['tf']
    df = d.information['df']
    length = d.information['document_length']
    one = math.log10((D + 0.5) / (df + 0.5))
    two = (tf + k1 * tf) / (tf + k1 * (1 - b + (b * length) / average))
    three = (tfq + k2 * tfq) / (tfq + k2)
    return one * two * three
# ----------------------------------------------------------
def Laplace(d):
    tf = d.information['tf']
    length = d.information['document_length']
    p = (tf + 1) / (length + V)
    p = math.log10(p)
    return p
def Laplace2(tf, length):
    # handle when the document was not hit
    global V
    p = (tf + 1) / float((length + V))
    p = math.log10(p)
    return p
# ----------------------------------------------------------
def Jelinek_Mercer(d, back_tf, back_len):
    f = 0.3
    tf = d.information['tf']
    length = d.information['document_length']
    p = f * (tf / length) + (1 - f) * (back_tf  / back_len)
    p = math.log10(p)
    return p
def Jelinek_Mercer2(length, back_tf, back_len):
    # handle when the document was not hit
    f = 0.3
    tf = 0
    p = f * (tf / length) + (1 - f) * (back_tf  / back_len)
    p = math.log10(p)
    return p
# ----------------------------------------------------------
def getPostings(keyword):
    result = es.search(index = 'ap_dataset', doc_type = 'document',
        body = {
        'query' : {
        'match':{
        'text':keyword
        }
        },
        # 'sort':'_id',
        'explain' : 'true',
        'size' : 10000})
       # q = 'text:'+keyword, size = 10000, explain = True)
    # print result
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
    Okapi_BM25_socres = collections.defaultdict(lambda:0.0)
    Laplace_scores = collections.defaultdict(lambda:0.0)
    Jelinek_Mercer_scores = collections.defaultdict(lambda:0.0)
    # this search will iterate every item in query
    for item in query:
        if item in recentsearch:
            posting = recentsearch[item]
        else:
            posting = getPostings(item)
            recentsearch[item] = posting
        # term frequency in query
        freq = termFrequencyInQuery(item, query)
        tf = 0.0
        length = 0.0
        hit = {}
        enter = False
        for doc in posting:
            docno = doc.information['docno']
            hit[docno] = 1
            Okapi_TF_scores[docno] += Okapi_TF(doc)
            TF_IDF_scores[docno] += TF_IDF(doc)
            Okapi_BM25_socres[docno] += Okapi_BM25(freq, doc)
            Laplace_scores[docno] += Laplace(doc)
            tf += doc.information['tf']
            length += doc.information['document_length']
        for doc in document_length:
            if doc in hit:
                continue
            Laplace_scores[doc] += Laplace2(0, int(document_length[doc]))
        for doc in posting:
            docno = doc.information['docno']
            Jelinek_Mercer_scores[docno] += Jelinek_Mercer(doc, tf, length)
            enter = True
        for doc in document_length:
            if enter == False:
                break
            if doc in hit:
                continue
            Jelinek_Mercer_scores[doc] += Jelinek_Mercer2(int(document_length[doc]), tf, length)
    Okapi_TF_scores = sort(Okapi_TF_scores, k)
    TF_IDF_scores = sort(TF_IDF_scores, k)
    Okapi_BM25_socres = sort(Okapi_BM25_socres, k)
    Laplace_scores = sort(Laplace_scores, k)
    Jelinek_Mercer_scores = sort(Jelinek_Mercer_scores, k)
    return [Okapi_TF_scores, TF_IDF_scores, Okapi_BM25_socres, Laplace_scores,
    Jelinek_Mercer_scores]
    # print scores
# ----------------------------------------------------------
def getQuestion():
    # read the question from file
    filecontent = indexing.readFile('./AP_DATA/query_desc.51-100.short.txt')
    questions = {}
    for q in filecontent:
        if 'Document' not in q:
            continue
        print q
        # get the question number
        num = q.split(' ')[0]
        num = num[0:len(num)-1]
        query = es.indices.analyze(index = 'ap_dataset', analyzer = 'my_english',
            text = q[len(num)+1:])
        query = [str(item['token']) for item in query['tokens']]
        questions[num]=query[3:]
    return questions
# ----------------------------------------------------------
def outputFormat(container, result, temp):
    # this function is used to give the right output format
    i = 1
    for r in result:
        temp_ = temp
        temp_ += r[0] + ' ' + str(i) + ' ' + str(r[1]) + ' Exp\n'
        container.append(temp_)
        i += 1
    return container
# ----------------------------------------------------------
def writeFile(container, filename):
    # this function is used to write result to the file
    f = open(filename, 'w')
    for content in container:
        f.writelines(content)
    f.close()
# ----------------------------------------------------------
def doSearch():
    # this function is used to get query from the query list
    # and send the query to the search function, when
    # get which is the right document, the function will
    # call function to write result to the file
    questions = getQuestion()
    Okapi_TF = []
    TF_IDF = []
    Okapi_BM25 = []
    Laplace_smoothing = []
    Jelinek_Mercer = []
    for index in questions:
        print 'search ', questions[index]
        temp = ''
        temp += index + ' Q0 '
        result = search(questions[index], K)
        Okapi_TF = outputFormat(Okapi_TF, result[0], temp)
        TF_IDF = outputFormat(TF_IDF, result[1], temp)
        Okapi_BM25 = outputFormat(Okapi_BM25, result[2], temp)
        Laplace_smoothing = outputFormat(Laplace_smoothing, result[3], temp)
        Jelinek_Mercer = outputFormat(Jelinek_Mercer, result[4], temp)
    writeFile(Okapi_TF, 'Okapi_TF')
    writeFile(TF_IDF, 'TF_IDF')
    writeFile(Okapi_BM25, 'Okapi_BM25')
    writeFile(Laplace_smoothing, 'Laplace_smoothing')
    writeFile(Jelinek_Mercer, 'Jelinek_Mercer')
# ----------------------------------------------------------
def getNeededInformationFromFile():
    # get necessary information, like total documents length,
    # V of the total documents from cached data
    f = open('cache', 'r')
    global V
    global average
    global document_length
    content = []
    for line in f:
        # extract \n
        line = line[:-1]
        content.append(line)
    f.close()
    V = int(content[0])
    average = float(content[1])
    i = 2
    for line in content:
        if 'AP' not in line:
            continue
        line = line.split(' ')
        docno = line[0]
        length = int(line[1])
        document_length[docno] = length
# ----------------------------------------------------------
def main():
    print 'get D, V, average document length'
    # getNeededInformation()
    if os.path.exists('./cache'):
        getNeededInformationFromFile()
    else:
        cache_data.cache()
        getNeededInformationFromFile()
    print D, V, average
    print 'start to perform search'
    doSearch()
# ----------------------------------------------------------
if __name__ == '__main__':
    main()
