#!/usr/bin/env python
import sys, os, re, uuid
import glob
import wtf

from elasticsearch import Elasticsearch
es = Elasticsearch()
# data definition
# f = open('storage_12573', 'r').readlines()
# l = open('linkgraph_12573', 'r').readlines()
hash_map = {}

class URL(object):
    """docstring for URL"""
    def __init__(self, url):
        super(URL, self).__init__()
        self.url = url
        self.in_num = 0
        self.out_num = 0
        self.in_links = []
        self.out_links = []


def handleTemp(temp, tag, end_tag):
    # extract content from <TEXT> and void <TEXT>content</TEXT>
    start = temp.find(tag) + len(tag)
    end = temp.find(end_tag)
    return temp[start:end]

# ----------------------------------------------------------------------------
def indexing(url, title, http_header, text, raw):
    print url
    if url not in hash_map:
        print 'not '
    in_links = map(wtf.url_to_uuid, hash_map[url].in_links)
    out_links = map(wtf.url_to_uuid, hash_map[url].out_links)
    doc = {
        'url' : url,
        'text': text,
        'html': raw,
        'header': http_header,
        'in-links': in_links,
        'out_links': out_links
    }
    es.index(index='hw3',
             doc_type = 'document',
             id = wtf.url_to_uuid(url),
             body = doc)
# ----------------------------------------------------------------------------
# handleDocument
def handleDocument(content):
    i = 0
    length = len(content)
    pattern = re.compile('<DOCNO>\s(.*?)\s</DOCNO>')
    docno = pattern.findall(content)[0]  # docno is the url
    pattern = re.compile('<HEAD>\s(.*?)\s</HEAD>')
    title = pattern.findall(content)[0]  # title of the document
    pattern = re.compile('<HTML-HEAD>\n(.*?)\n<\/HTML-HEAD>')  # http header
    http_header = pattern.findall(content)[0]
    text = handleTemp(content, '<TEXT>', '</TEXT>')
    raw = handleTemp(content, '<RAW>', '</RAW>')
    indexing(docno, title, http_header, text, raw)
# ----------------------------------------------------------------------------
# split the file into document
def splitDoc(content):
    length = len(content)
    i = 0
    while i < length:
        if '<DOC>' in content[i]:
            # start to get the whole doc
            doc = ''
            i += 1
            while '</DOC>' not in content[i]:
                doc += content[i]
                i += 1
            handleDocument(doc)
        i += 1
    # print i
# ----------------------------------------------------------------------------
def main():
    for line in l:
        line = line.split()
        if len(line) < 1:
            continue
        url = line[0]
        hash_map[url] = URL(url)

        hash_map[url].out_links = line[1 : len(line)]
        # print hash_map[url].in_links, hash_map[url].out_links
    splitDoc(f)
    # print count
if __name__ == '__main__':
    # main()
    print check_exist('http://en.wikipedia.org/wiki/Rhys_Ifans')
