#!/usr/bin/env python
# import numpy as linalg
import numpy
from scipy.sparse import csc_matrix
from scipy.sparse import linalg
from scipy.sparse import lil_matrix
from scipy.sparse.linalg import spsolve
import time
import sys
import collections
import operator
# import scipy.sparse as sparse
count = 0
hash_map = {}  # url -> id
hash_map_2  = {} # id -> url
# hash_set = set()


class Node(object):
    """docstring for Node"""
    def __init__(self, arg):
        super(Node, self).__init__()
        self.url = arg  # the url is a identifier for the node
        self.outlink = set()  # the out link for the node
        pass


def read_file(file_name):
    f = open(file_name, 'r').readlines()
    # get the # for each node
    global count
    for line in f:
        line = line.split()
        node = line[0]
        if node in hash_map:
            continue
        else:
            hash_map[node] = count
            hash_map_2[count] = node
            count += 1
    data, row, col = [], [], []
    for line in f:
        line = line.split()
        num = 0
        row_num = hash_map[line[0]]
        for i in range(1, len(line)):
            if line[i] not in hash_map:
                continue
            num += 1
        for i in range(1, len(line)):
            if line[i] not in hash_map:
                continue
            column_num = hash_map[line[i]]
            row.append(row_num)
            col.append(column_num)
            data.append(float(1)/num)
    my_list = [1.0/len(hash_map)] * len(hash_map)
    build_sparse(numpy.array(data), numpy.array(row), numpy.array(col), len(hash_map), numpy.array(my_list))
    pass


def dump_file(file_name):
    f = open(file_name, 'r').readlines()
    # we need a list to store node obj
    node_map = {}
    for line in f:
        line = line.split()
        head = line[0]
        for i in range(1, len(line)):
            if line[i] in node_map:
                node_map[line[i]].add(head)
            else:
                node_map[line[i]] = set()
                node_map[line[i]].add(head)
    out_links = open('out_link.txt', 'w')
    for key in node_map:
        result = key + ' '
        for node in node_map[key]:
            result += node + ' '
        out_links.writelines(result + '\n')
    out_links.close()
    pass


def build_sparse(data, row, col, n, my_list):
    sparse_matrix = csc_matrix((data, (row, col)), shape=(n, n))
    print 'start to calculate'
    # initial = numpy.asmatrix(numpy.full((sparse_matrix.shape[0], 1), 1 / sparse_matrix.shape[0]))
    for i in range(100):
        temp = my_list
        my_list = my_list * sparse_matrix
        if i > 50 and numpy.linalg.norm(my_list - temp, 1) < 1e-8:
            break
    res = {}
    out = open('result_page.txt', 'w')
    for num in range(0, len(hash_map)):
        res[hash_map_2[num]] = my_list[num]
    res = sorted(res.items(), key=lambda x: (-x[1]))
    for i in res:
        out.writelines(i[0] + ' ' + str(i[1]) + '\n')
    out.close()



def eigen_example():
    A = csc_matrix([[0,1], [1,0]],dtype=float)
    # print A
    # vals, vecs = linalg.eigs(A, k =1)
    # print vecs
    my_list = [1.0/2.0] * 2
    print my_list
    temp = A.dot(my_list)
    for i in range(100):
        temp = A.dot(temp)
    print temp


def page_rank_crawled():
    read_file('linkgraph_12573')


if __name__ == '__main__':
    dump_file('wt2g_inlinks.txt')
    read_file('out_link.txt')
    # page_rank_crawled()
    pass

# eigen_example()




