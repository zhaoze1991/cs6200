#!/usr/local/bin/python
import sys, os
import glob
from elasticsearch import Elasticsearch
es = Elasticsearch()
# data definition
files = []
path = './AP_DATA/ap89_collection/*'
count = 0
# ----------------------------------------------------------------------------
def getFileName():
    global files
    for file in glob.glob(path):
        files.append(file)
    # remove readme from the file
    del files[-1]
# ----------------------------------------------------------------------------
# read file by filename
def readFile(fileName):
    f = open(fileName, 'r')
    content = []
    for line in f:
        content.append(line)
    f.close()
    return content
# ----------------------------------------------------------------------------
def handleTemp(temp):
    # extract content from <TEXT> and void <TEXT>content</TEXT>
    start = temp.find('<TEXT>') + len('<TEXT>')
    end = temp.find('</TEXT>')
    return temp[start:end]
# ----------------------------------------------------------------------------
def indexing(documentid, Text):
    doc = {
    'docno' : documentid,
    'text': Text
    }
    global count
    es.index(index='ap_dataset', doc_type = 'document', id = count, body = doc)
# ----------------------------------------------------------------------------
# handleDocument
def handleDocument(content):
    documentid = ''
    Text = ''
    i = 0
    length = len(content)
    while i < length:
        if '<DOCNO>' in content[i]:
            no = content[i].split(' ')
            documentid = no[1]
            break
            # find the doc id, and to avoid un-necessary if-check, end this loop
        i += 1
    while i < length:
        if '<TEXT>' in content[i]:
            temp = ''
            while '</TEXT>' not in content[i]:
                # replace the '\n' with space
                temp += content[i][:-1] + ' '
                i += 1
            temp += content[i]
            Text += handleTemp(temp)
        i += 1
    indexing(documentid, Text)
# ----------------------------------------------------------------------------
# split the file into document
def splitDoc(content):
    length = len(content)
    i = 0
    global count
    while i < length:
        if '<DOC>' in content[i]:
            # start to get the whole doc
            doc = []
            doc.append(content[i])
            i += 1
            while '</DOC>' not in content[i]:
                doc.append(content[i])
                i += 1
            doc.append(content[i])
            handleDocument(doc)
            count += 1
        i += 1
    # print i
# ----------------------------------------------------------------------------
def main():
    getFileName()
    for file in files:
        content = readFile(file)
        splitDoc(content)
        # how many documents have been processed
        print count
if __name__ == '__main__':
    main()
