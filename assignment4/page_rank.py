#!/usr/bin/env python
# import numpy as linalg
import numpy
from scipy.sparse import csc_matrix
from scipy.sparse import linalg
from scipy.sparse import lil_matrix
from scipy.sparse.linalg import spsolve
import time
import sys
# import scipy.sparse as sparse
count = 0
hash_map = {}  # page id -> count


class Node(object):
    """docstring for Node"""
    def __init__(self, arg):
        super(Node, self).__init__()
        self.url = arg  # the url is a identifier for the node
        self.id = 0  # the id of the node.
        self.outlink_num = 0
        self.outlink = set()  # the out link for the node


def read_file():
    f = open('wt2g_inlinks.txt', 'r').readlines()
    # get the # for each node
    global count
    for line in f:
        line = line.split()
        for item in line:
            if item in hash_map:
                continue
            else:
                hash_map[item] = count
                count += 1
    # get let's construct the matrix
    data, row, col = [], [], []
    for line in f:
        line = line.split()
        num = len(line) - 1
        row_num = hash_map[line[0]]
        for i in range(1, len(line)):
            column_num = hash_map[line[i]]
            row.append(row_num)
            col.append(column_num)
            data.append(float(1)/num)
    sparse_matrix = csc_matrix((numpy.array(data), (numpy.array(row), numpy.array(col))), shape=(len(hash_map), len(hash_map))).toarray()
    # my_list = [float(1)/len(hash_map)] * len(hash_map)
    # v = numpy.array(my_list)
    # v = sparse_matrix.dot(v)
    # for i in range(20):
    #     v = sparse_matrix.dot(v)
    # for i in v:
    #     print i
    print sparse_matrix
    vals, vecs = linalg.eigs(sparse_matrix, k = 1)
    print vecs

def eigen_example():
    A = csc_matrix([[1,0,0], [0,1,0],[0,0,1]],dtype=float)
    print A
    vals, vecs = linalg.eigs(A, k =1)
    print vals, vecs
    print A
    print vecs.shape
# eigen_example()
# A =csc_matrix([[1,0,0]])
# B = csc_matrix([[0,1,0]])
# C = csc_matrix([0,0,1])
# A += B
# A += C
# print A
read_file()
