#!/usr/bin/env python
import numpy, mlpy, sys
from itertools import islice


class MyMatrix(object):
    def __init__(self, qid):
        self.qid = qid
        self.tf_idf, self.bm25, self.okapi = {}, {}, {}
        self.jel, self.lap, self.label = {}, {}, {} # docid -> corresponding score
        self.category = ''
    pass

matrix_hash = {}  # qid -> mymatirx object
svm = mlpy.LibLinear(solver_type='l1r_lr')
ff = open('predict_train', 'w')
def dump_training():
    f = open('feature_matirx', 'r').readlines()
    x, y = [], []
    for line in f:
        line = line.split()
        if line[9] == 'test':
            continue
        x_temp, y_temp, i = [], [], 2
        while i < 8:
            x_temp.append(float(line[i]))
            i += 1
        x.append(x_temp)
        y.append(int(line[8]))
    svm.learn(numpy.array(x), numpy.array(y))
    for line in f:
        line = line.split()
        if line[9] == 'train':
            continue
        x_temp, i = [], 2
        while i < 7:
            x_temp.append(float(line[i]))
            i += 1
#         print line[0]
    del f


def write_to_file(qid, hash_map):
    res = sorted(hash_map.items(), key = lambda x:(-x[1]))
    result = list(islice(res, 100))
    count = 1
    for item in result:
        res = qid + ' Q0 '+ item[0] + ' ' + str(count) + ' ' + str(item[1]) + ' Exp\n'
        ff.writelines(res)
        count += 1
    pass

def test_tranning():
    f = open('feature_matirx', 'r').readlines()
    hash_map = {}
    qid  = '54'
    for line in f:
        if 'test' in line:
            continue
        line = line.split()
        if line[0] != qid:
            write_to_file(qid, hash_map)
            hash_map = {}
            qid = line[0]
        x, i = [], 2
        while i < 8:
            x.append(float(line[i]))
            i += 1
        pred = svm.pred_probability(numpy.array(x))
        hash_map[line[1]] = pred[1]
    write_to_file(qid, hash_map)
    pass

def test_testing():
    f = open('feature_matirx', 'r').readlines()
    hash_map = {}
    qid  = '54'
    for line in f:
        if 'train' in line:
            continue
        line = line.split()
        if line[0] != qid:
            write_to_file(qid, hash_map)
            hash_map = {}
            qid = line[0]
        x, i = [], 2
        while i < 8:
            x.append(float(line[i]))
            i += 1
        pred = svm.pred_probability(numpy.array(x))
        hash_map[line[1]] = pred[1]
    write_to_file(qid, hash_map)
    pass

if __name__ == '__main__':
    dump_training()
    test_tranning()
    ff = open('predict_test', 'w')
    test_testing()

