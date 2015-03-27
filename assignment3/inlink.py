#!/usr/bin/env python
import re
import time
import wtf
import uuid
from elasticsearch import Elasticsearch
hash_set = set()
hash_map = {}   # check if one of the url exists
result = {}  # url -> hash_set() means one url has


def find_docno(filename):
    f1 = str(open(filename, 'r').readlines())
    pattern = re.compile('<DOCNO>\s(.*?)\s</DOCNO>')
    docno = pattern.findall(f1)
    del f1
    for doc in docno:
        hash_set.add(doc)
    pass


def dump_url(filename):
    f1 = open(filename, 'r').readlines()
    for line in f1:
        line = line.split()
        hash_map[line[0]] = set()
        i = 1
        while i < len(line):
            if line[i] not in hash_set:
                i += 1
                continue
            hash_map[line[0]].add(line[i])
            i += 1
    del f1



def my_func():
    es = Elasticsearch()
    # TODO add more files here
    find_docno('storage_12573')
    dump_url('linkgraph_12573')

    for link in hash_set:
        result[link] = set()
        for key in hash_map:
            if link in hash_map[key]:
                result[link].add(key)
        update_es(es, link, result[link])


def update_es(es, url, ins):
    inns = map(wtf.url_to_uuid, ins)
    doc = {
        'doc': {
            'in-links': inns,
        }
    }
    try:
        es.update(index='hw3',
                  id=str(uuid.uuid5(uuid.NAMESPACE_URL, url)),
                  doc_type='document',
                  body=doc)
    except:
        print url, 'shit'



if __name__ == '__main__':
    my_func()
