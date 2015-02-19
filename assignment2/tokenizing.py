#!/usr/local/bin/python
import os, sys, re, collections
import glob
import snowballstemmer
import documents
# data definition
files = []
path = './AP_DATA/ap89_collection/*'
# term -> id
term_id = {}
# id -> term
id_term = {}
# term -> [(docno, position)]
hash_map = {}
space = ' '
cf = {}
df = {}
vf = {}
document_id = 0
document_num = 0

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

def stem(word):
    # static variable
    stem.stemmer = snowballstemmer.stemmer('english')
    word = stem.stemmer.stemWord(word)
    return word

def tokenizing(documents):
    global document_id
    for document in documents:
        tf = {}
        words = {}
        docno = document[0]
        content = document[1]
        pattern = re.compile('\w+(?:\.?\w+)*')
        tokens = pattern.findall(content)
        result = []
        position = 1
        print document_id
        id_term[document_id] = docno
        for t in tokens:
            t = t.lower()
            if t in words:
                words[t].append(position)
                tf[t] += 1
            else:
                words[t] = [position]
                tf[t] = 1
                # unique term in documents
                if t in vf:
                    pass
                else:
                    vf[t] = 1
            if t in cf:
                cf[t] += 1
            else:
                cf[t] = 1
            position += 1
        for key in words:
            # key is the term
            if key in df:
                df[key] += 1
            else:
                df[key] = 1
            if key in hash_map:
                tuples = (tf[key], ) + tuple(words[key])
                # hash_map[key] = {}
                hash_map[key][document_id] = tuples
            else:
                tuples = (tf[key], ) + tuple(words[key])
                hash_map[key] = {}
                hash_map[key][document_id] = tuples
        document_id += 1

def sort():
    global hash_map
    result = sorted(hash_map.items(), key=lambda x: x[0])
    return result

def writefile(fileName, content):
    cache = open(fileName, 'wa')
    category = open(fileName+'Category','wa')
    start = 0
    for block in content:
        # (term, blabla)
        term = block[0]
        string = term + space + str(df[term]) + space + str(cf[term]) + space
        string += str(block[1]) + '\n'
        cache.write(string)
        cate = term + space + str(start) + space + str(len(string)) + '\n'
        category.write(cate)
        start += len(string)
    cache.close()
    category.close()




def indexing():
    result = sort()
    if os.path.exists('cache1') == False:
        writefile('cache1', result)
        return
    if os.path.exists('cache2') == False:
        writefile('cache2', result)
        # mergefile()
        sys.exit(-1)


getFileName()
result = []
for f in files:
    c = readFile(f)
    document = documents.splitDoc(c)
    size = len(document)
    if document_num + size >= 1000:
        indexing()
        document_num = 0
        hash_map = {}
        df = {}
    else:
        document_num += size
        tokenizing(document)
