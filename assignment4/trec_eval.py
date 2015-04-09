#!/usr/bin/env python
from elasticsearch import Elasticsearch
from math import log10
import sys
from collections import defaultdict
query_size = 100
ranklist = 'ranklist.txt'
qrel_map = {}  # query id -> result object
trec_map = {}  # query id -> query object
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
        self.f1 = {}  # k -> f1-measure
        self.relevant = 0
        self.avg = 0.0
        self.r_precision = 0.0  # R-Precision
        self.ndcg = 0.0  # nDCG
        self.recall_precision = {}

def get_ranklist():
    es = Elasticsearch()
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


def read_qrel(qrel):
    f = open(qrel, 'r').readlines()
    for line in f:
        # query id, meaningless, document id, grade
        line = line.split()
        qid = line[0]
        if qid not in qrel_map:
            qrel_map[qid] = Result(qid)
        qrel_map[qid].total_num += 1
        qrel_map[qid].hash_map[line[2]] = int(line[3])
        if int(line[3]) > 0:
            qrel_map[qid].relevant += 1
    pass


def dcg(r):
    r1, i = float(r[0]), 1
    while i < len(r):
        r1 += float(r[i] / log10(i + 1))
        i += 1
    return r1
    pass


def get_grade(qid, doc_id):
    qrel_obj = qrel_map[qid]
    if doc_id in qrel_obj.hash_map:
        return qrel_obj.hash_map[doc_id]
    else:
        return 0
    pass


def ndcg(obj):
    qid = obj.qid
    r = []
    for doc in obj.documents:
        r.append(get_grade(qid, doc))
    up = dcg(r)
    r.reverse()
    down = dcg(r)
    if down == 0:
        return
    obj.ndcg = up / down
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
    # obj.precision[k] = float(tp / k)
    return float(tp / k)
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
    # obj.recall[k] = tp / (res_obj.relevant)
    return float(tp / (res_obj.relevant))
    pass


def read_trec_file(filename):
    f = open(filename, 'r').readlines()
    for line in f:
        # query id, meaningless, document id, rank, score, meaningless
        line = line.split()
        qid = line[0]
        if qid not in trec_map:
            trec_map[qid] = Query(qid)
        trec_map[qid].total_num += 1
        trec_map[qid].documents.append(line[2])
        if line[2] in qrel_map[qid].hash_map and qrel_map[qid].hash_map[line[2]] > 0:
            trec_map[qid].relevant += 1
    pass


def f1_at_k(obj, k):
    precision = obj.precision[k]
    recall = obj.recall[k]
    obj.f1[k] = 2 * precision * recall / (precision + recall)
    pass


def avg_precision(obj):
    i, qid, k, total = 0, obj.qid, 0.0, 0.0
    for document in obj.documents:
        i += 1
        if document in qrel_map[qid].hash_map and qrel_map[qid].hash_map[document] > 0:
            k += 1.0
            total += float(k / i)
    obj.avg = float(total / qrel_map[qid].relevant)
    pass


def r_precision(obj):
    # check wiki about R-precision
    qid, i, r1 = obj.qid, 0, 0.0
    r2 = qrel_map[qid].relevant
    while i < r2 and i < len(obj.documents):
        doc = obj.documents[i]
        if doc in qrel_map[qid].hash_map and qrel_map[qid].hash_map[doc] > 0:
            r1 += 1
        i += 1
    obj.r_precision = float(r1 / r2)
    pass


def recall_precision(obj):
    rec_map = {
        0.0 : 0.0,
        0.2 : 0.0,
        0.4 : 0.0,
        0.6 : 0.0,
        0.8 : 0.0,
        1.0 : 0.0
    }
    qid = obj.qid
    qrel_obj = qrel_map[qid]
    for k in range(1, len(obj.documents)):
        recall = recall_at_k(obj, k)
        precision = precision_at_k(obj, k)
        for key in rec_map:
            if recall >= key and rec_map[key] < precision:
                rec_map[key] = precision
    return rec_map

def print_func(qid, ret, rel, rel_ret, p0, p2, p4, p6, p8, p10, avg, p5d, p10d, p20d, p50d, p100d, rp):
    print "\nQueryid (Num):    %5d" % qid
    print "Total number of documents over all queries"
    print "    Retrieved:    %5d" %ret
    print "    Relevant:     %5d" %rel
    print "    Rel_ret:      %5d" %rel_ret
    print "Interpolated Recall - Precision Averages:"
    print "    at 0.00       %.4f" %p0
    print "    at 0.20       %.4f" %p2
    print "    at 0.40       %.4f" %p4
    print "    at 0.60       %.4f" %p6
    print "    at 0.80       %.4f" %p8
    print "    at 1.00       %.4f" %p10
    print "Average precision (non-interpolated) for all rel docs(averaged over queries)";
    print "                  %.4f" %avg
    print "Precision:"
    print "  At    5 docs:   %.4f" %p5d
    print "  At   10 docs:   %.4f" %p10d
    print "  At   20 docs:   %.4f" %p20d
    print "  At   50 docs:   %.4f" %p50d
    print "  At  100 docs:   %.4f" %p100d
    print "R-Precision (precision after R (= num_rel for a query) docs retrieved):";
    print "    Exact:        %.4f" %rp

if __name__ == '__main__':
    arguments = sys.argv[1:]
    n = len(arguments)
    if n < 2:
        print USAGE
        sys.exit(-1)
    step = False
    if arguments[0] == '-q':
        step = True
    qrel, trec = arguments[n-2], arguments[n-1]
    read_qrel(qrel)
    read_trec_file(trec)
    res = {
        0.0 : 0.0,
        0.2 : 0.0,
        0.4 : 0.0,
        0.6 : 0.0,
        0.8 : 0.0,
        1.0 : 0.0
    }
    count, avg, tt_ndcg, recall, precision, tt_r_precision = 0, 0.0, 0.0, defaultdict(lambda:0.0), defaultdict(lambda:0.0), 0.0
    for key in trec_map:
        obj = trec_map[key]
        count += 1
        avg_precision(obj)
        avg += obj.avg
        r_precision(obj)
        tt_r_precision += obj.r_precision
        ndcg(obj)  # this function already modify the ndcg value
        tt_ndcg += obj.ndcg
        obj.recall_precision = recall_precision(obj)
        for key in obj.recall_precision:
            res[key] += obj.recall_precision[key]
        for k in [5, 10, 20, 50, 100]:
            obj.recall[k] = recall_at_k(obj, k)
            recall[k] += obj.recall[k]
            obj.precision[k] = precision_at_k(obj, k)
            precision[k] += obj.precision[k]
        if step:
            print_func(int(obj.qid), obj.total_num, qrel_map[obj.qid].relevant, obj.relevant,
                       obj.recall_precision[0.0], obj.recall_precision[0.2],
                       obj.recall_precision[0.4], obj.recall_precision[0.6], obj.recall_precision[0.8],
                       obj.recall_precision[1.0], obj.avg, obj.precision[5],
                       obj.precision[10], obj.precision[20],
                       obj.precision[50], obj.precision[100],
                       obj.r_precision)
        pass
    Retrieved = reduce(lambda x, y : x + y, map(lambda x: x.total_num, trec_map.values()))
    Relevant = reduce(lambda x, y : x + y, map(lambda x: x.relevant, qrel_map.values()))
    Rel_ret = reduce(lambda x, y : x + y, map(lambda x: x.relevant, trec_map.values()))
    print_func(len(trec_map), Retrieved, Relevant, Rel_ret,
               res[0.0] / count, res[0.2] / count,
               res[0.4] / count, res[0.6] / count, res[0.8] / count,
               res[1.0] / count,  avg / count,  precision[5] / count,
               precision[10] / count, precision[20] / count,
               precision[50] / count, precision[100] / count,
               tt_r_precision / count)
