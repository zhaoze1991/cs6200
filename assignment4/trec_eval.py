#!/usr/bin/env python
from elasticsearch import Elasticsearch
es = Elasticsearch()
query_size = 100
ranklist = 'ranklist.txt'
qrel_name = 'qrels.adhoc.51-100.AP89.txt'  # these two file name should be get from terminal
qrel_map = {}  # query id -> result object
trec_map = {}  # query id -> query object
trec_file = 'tt_TF_IDF'
USAGE = 'Usage:  trec_eval [-q] <qrel_file> <trec_file>'

class Result(object):
    """docstring for Result"""
    def __init__(self, qid):
        super(Result, self).__init__()
        self.qid = qid  # query id
        self.hash_map = {}  # document id -> grade
        self.relevant = 0  # number of document that was relevant
        self.total_num = 0  # number of document marked 0 or 1


class Query(object):
    """docstring for Query"""
    def __init__(self, qid):
        super(Query, self).__init__()
        self.qid = qid  # query id
        self.documents = []  # documents that ranked in order
        self.total_num = 0
        # k=5,10, 20, 50, 100
        self.precision = {}  # k -> precision
        self.recall = {}  # k -> recall


def get_ranklist():
    query = {
        23333 : 'fuck you',
        }
    f = open(ranklist, 'w')
    for key in query:
        result = es.search(index = 'hw3', doc_type = 'document',
                           body = {
                               "query" : {
                                   "query_string": {
                                       "query" : query[key]
                                   }
                               },
                               "size":query_size,
                               "fields": ['url']
                           })
        result = result['hits']['hits']
        rank = 1
        for document in result:
            res = str(key) + ' zzz ' + document['fields']['url'][0].encode('utf-8') + ' ' + str(rank) + ' ' + str(document['_score']) + ' Exp\n'
            rank += 1
            f.writelines(res)
    f.close()


def read_qrel():
    f = open(qrel_name, 'r').readlines()
    for line in f:
        # query id, meaningless, document id, grade
        line = line.split()
        qid = line[0]
        if qid not in qrel_map:
            qrel_map[qid] = Result(qid)
        qrel_map[qid].total_num += 1
        qrel_map[qid].hash_map[line[2]] = int(line[3])
        if line[3] > 0:
            qrel_map[qid].relevant += 1
    pass


def precision_at_k(obj, k):
    # tp / (tp + tf)
    res_obj = qrel_map[obj.qid]
    i = 0
    tp = 0.0
    while i < k:
        doc = obj.documents[i]
        if doc in res_obj.hash_map and res_obj.hash_map[doc] > 0:
            tp += 1
        i += 1
    obj.precision[k] = float(tp / k)
    pass


def recall_at_k(obj, k):
    # tp / (tp + fn)
    res_obj = qrel_map[obj.qid]
    i, tp = 0, 0.0
    while i < k:
        doc = obj.documents[i]
        if doc in res_obj.hash_map and res_obj.hash_map[doc] > 0:
            tp += 1
        i += 1
    obj.recall[k] = tp / (tp + res_obj.relevant)
    pass


def read_trec_file(filename):
    f = open(trec_file, 'r').readlines()
    for line in f:
        # query id, meaningless, document id, rank, score, meaningless
        line = line.split()
        qid = line[0]
        if qid not in trec_map:
            trec_map[qid] = Query(qid)
        trec_map[qid].total_num += 1
        trec_map[qid].documents.append(line[2])
        pass
    pass


if __name__ == '__main__':
    read_qrel()
    read_trec_file('hello')
    for k in [5, 10, 20, 50, 100]:
        for key in trec_map:
            recall_at_k(trec_map[key], k)
            precision_at_k(trec_map[key], k)

    p5, p10, p20, p50, p100, count = 0.0, 0.0, 0.0, 0.0, 0.0, 0
    for key in trec_map:
        count += 1
        obj = trec_map[key]
        p5 += obj.precision[5]
        p10 += obj.precision[10]
        p20 += obj.precision[20]
        p50 += obj.precision[50]
        p100 += obj.precision[100]
    print count, p5 / count, p10 / count, p20/count, p50/count, p100/count
    print count, p5, p10, p20, p50, p100








