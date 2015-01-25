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
recentsearch = {}
D = es.count(index = 'ap_dataset')['count'] # D is the total number of documents
K = 100                        # K is the number of documents should be returned
V = 10000000
average = 200.0
document_length = {}
all_item = []
class Document(object):
    """docstring for Document"""
    def __init__(self, the_id, info):
        super(Document, self).__init__()
        self.id = the_id
        self.information = info
# ----------------------------------------------------------
def getNeededInformation():
    _id = 0
    avg = 0.0
    global D
    n = 0
    while _id < D :
        term = es.termvector(index = 'ap_dataset', doc_type='document', id = _id)
        if 'text' not in term['term_vectors']:
            _id += 1
            continue
        term = term['term_vectors']['text']['terms']
        total = 0
        # get the length of the document
        for key in term.items():
            freq = key[1]['term_freq']
            total += freq
        term = term.keys()
        for t in term:
            t = t.encode('UTF-8')
            if t in all_item:
                continue
            all_item.append(t)
            # print t
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
        avg += total
        _id += 1
    global average
    average = avg / n
    global V
    V =len(all_item)
    save = open('cache','w')
    save.writelines(str(V) +'\n')
    save.writelines(str(average) + '\n')
    for key in document_length:
        string = key + ' ' + str(document_length[key])+'\n'
        save.writelines(string)
    save.close()
def handleJson(details):
    if 'description' in details and 'fieldWeight' in details['description']:
        for d in details['details']:
            if 'tf(' not in d['description']:
                continue
            for f in d['details']:
                if 'termFreq' not in f['description'] and 'phraseFreq' not in f['description']:
                    continue
                return f['value']

    for detail in details['details']:
        if 'fieldWeight' not in detail['description']:
            continue
        for d in detail['details']:
            if 'tf(' not in d['description']:
                continue
            for f in d['details']:
                if 'termFreq' not in f['description'] and 'phraseFreq' not in f['description']:
                    continue
                return f['value']
    return -1
# ----------------------------------------------------------
def getInformation(keyword, result):
    df = result['hits']['total']
    output = []
    total = 0.0
    num = 0
    for hit in result['hits']['hits']:
        the_id = hit['_id'].encode('UTF-8')
        term = es.termvector(index  ='ap_dataset', doc_type = 'document', id=the_id)
        docno = hit['_source']['docno'].encode('UTF-8')
        # print the_id
        # tf = hit['_explanation']['details'][0]['details'][1]['details'][0]['details'][0]['value']
        # print hit['_explanation']['details'][0]
        details = hit['_explanation']['details'][0]
        tf = handleJson(details)
        temp = {
            'document_length': document_length[docno],
            'docno': docno,
            'tf': tf,
            'df': df
        }
        # print keyword, temp
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
def Okapi_TF(d):
    global average
    tf = d.information['tf']
    # TODO double-check if the length is correct
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
def Laplace(d):
    tf = d.information['tf']
    length = d.information['document_length']
    p = (tf + 1) / (length + V)
    p = math.log10(p)
    return p
def Jelinek_Mercer(d, back_tf, back_len):
    f = 0.3
    tf = d.information['tf']
    length = d.information['document_length']
    p = f * (tf / length) + (1 - f) * ((back_tf - tf) / (back_len -length))
    p = math.log10(p)
    return p
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
# def sort1(data, k):

# ----------------------------------------------------------
def search(query, k):
    Okapi_TF_scores = collections.defaultdict(lambda:0.0)
    TF_IDF_scores = collections.defaultdict(lambda:0.0)
    Okapi_BM25_socres = collections.defaultdict(lambda:0.0)
    Laplace_scores = collections.defaultdict(lambda:0.0)
    Jelinek_Mercer_scores = collections.defaultdict(lambda:0.0)
    for item in query:
        if item not in recentsearch.keys():
            posting = getPostings(item)
            recentsearch[item] = posting
        else:
            posting = recentsearch[item]
        # term frequency in query
        freq = termFrequencyInQuery(item, query)
        tf = 0.0
        length = 0.0
        for doc in posting:
            docno = doc.information['docno']
            Okapi_TF_scores[docno] += Okapi_TF(doc)
            TF_IDF_scores[docno] += TF_IDF(doc)
            Okapi_BM25_socres[docno] += Okapi_BM25(freq, doc)
            Laplace_scores[docno] += Laplace(doc)
            tf += doc.information['tf']
            length += doc.information['document_length']
        for doc in posting:
            docno = doc.information['docno']
            Jelinek_Mercer_scores[docno] += Jelinek_Mercer(doc, tf, length)
    Okapi_TF_scores = sort(Okapi_TF_scores, k)
    TF_IDF_scores = sort(TF_IDF_scores, k)
    Okapi_BM25_socres = sort(Okapi_BM25_socres, k)
    Laplace_scores = sort(Laplace_scores, k)
    Jelinek_Mercer_scores = sort(Jelinek_Mercer_scores, k)
    return [Okapi_TF_scores, TF_IDF_scores, Okapi_BM25_socres, Laplace_scores,
    Jelinek_Mercer_scores]
    # print scores
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
    writeFile(Okapi_TF, 'Okpai_TF')
    writeFile(TF_IDF, 'TF_IDF')
    writeFile(Okapi_BM25, 'Okapi_BM25')
    writeFile(Laplace_smoothing, 'Laplace_smoothing')
    writeFile(Jelinek_Mercer, 'Jelinek_Mercer')
def getNeededInformationFromFile():
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

def main():
    print 'get D, V, average document length'
    # getNeededInformation()
    getNeededInformationFromFile()
    print D, V, average
    print 'start to perform search'
    doSearch()
if __name__ == '__main__':
    main()
