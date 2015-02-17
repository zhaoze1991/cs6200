#!/usr/local/bin/python
import os, sys, re
import glob
import snowballstemmer
import documents
# data definition
files = []
path = './AP_DATA/ap89_collection/*'

def getFileName():
    global files
    for file in glob.glob(path):
        files.append(file)
    # remove readme from the file
    del files[-1]

# read file by filename
def readFile(fileName):
    f = open(fileName, 'r')
    content = f.readlines()
    f.close()
    return content


termid = {}
_id = 1
document_id = 1

def stem(word):
    # static variable
    stem.stemmer = snowballstemmer.stemmer('english')
    word = stem.stemmer.stemWord(word)
    return word

def tokenizing(documents):
    for document in documents:
        docno = document[0]
        content = document[1]
        pattern = re.compile('\w+\.?\w*')
        tokens = pattern.findall(content)
        result = []
        position = 1
        global _id
        for t in tokens:
            t = t.lower()
            if '.' == t[len(t) - 1]:
                t = t[:-1]
            if t in termid:
                tuples = (t, docno, termid[t], position)
                result.append(tuples)
            else:
                termid[t] = _id
                _id += 1
                tuples = (t, docno, termid[t], position)
                result.append(tuples)
            position += 1
        print result

getFileName()
for f in files:
    c = readFile(f)
    documents = documents.splitDoc(c)
    tokenizing(documents)
    sys.exit(-1)
