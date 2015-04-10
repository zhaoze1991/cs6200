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


def read_file():
    f = open('out_link.txt', 'r').readlines()
    # get the # for each node
    global count
    for line in f:
        line = line.split()
        for item in line:
            if item in hash_map:
                continue
            else:
                hash_map[item] = count
                hash_map_2[count] = item
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
    my_list = [float(1)/len(hash_map)] * len(hash_map)
    build_sparse(numpy.array(data), numpy.array(row), numpy.array(col), len(hash_map), my_list)
    # v = numpy.array(my_list)
    # v = sparse_matrix.dot(v)
    # for i in range(20):
    #     v = sparse_matrix.dot(v)
    # for i in v:
    #     print i
    # print sparse_matrix



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
    print sparse_matrix
    for i in range(100):
        sparse_matrix = sparse_matrix.multiply(sparse_matrix)
    res = sparse_matrix.dot(my_list)
    out = open('result_page.txt', 'w')
    for num in range(0, len(hash_map)):
        result = hash_map_2[num] + ' '
        result += str(res[num]) + '\n'
        out.writelines(result)
    out.close()



def eigen_example():
    A = csc_matrix([[1,0,0], [0,1,0],[0,0,1]],dtype=float)
    print A
    vals, vecs = linalg.eigs(A, k =1)
    print vals, vecs
    print A
    print vecs.shape


if __name__ == '__main__':
    dump_file('wt2g_inlinks.txt')
    read_file()




