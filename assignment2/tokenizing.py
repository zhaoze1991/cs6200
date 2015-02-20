#!/usr/local/bin/python
import os, sys, re, collections, struct
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
    global hash_map
    global df
    global cf
    for document in documents:
        tf = {}
        words = {}
        docno = document[0]
        content = document[1]
        pattern = re.compile('\w+(?:\.?\w+)*')
        tokens = pattern.findall(content)
        result = []
        position = 1
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


def writefile_v2(fileName, content):
    cache = open(fileName, 'wa')
    category = open(fileName+'Category','wa')
    start = 0
    # term, df, cf, list
    for block in content:
        # (term, blabla)
        # print block
        term = block[0]
        string = term + space + str(df[term]) + space + str(cf[term]) + space
        for b in block[1]:
            string += str(b) + space
            for i in block[1][b]:
                string += str(i) + space
        # print string
        # sys.exit(-1)
        string = string[:-1]
        string += '\n'
        cache.write(string)
        cate = term + space + str(start) + space + str(len(string)) + '\n'
        category.write(cate)
        start += len(string)
    cache.close()
    category.close()


def indexing():
    result = sort()
    if os.path.exists('cache1') == False:
        writefile_v2('cache1', result)
        return
    if os.path.exists('cache2') == False:
        writefile_v2('cache2', result)
        documents.mergefile()

def cachemore():
    global id_term
    # result = sorted(id_term.items(), key=lambda x: x[0])
    documentid = open('documentid','w')
    for key in id_term:
        content = str(key) + ' ' + id_term[key] + '\n'
        documentid.write(content)

def doIndex():
    indexing()
    global document_num
    global hash_map
    global df
    global cf
    hash_map = {}
    df = {}
    cf = {}
    document_num = 0

def main():
    getFileName()
    no = 1
    global document_num
    for f in files:
        print no, document_num
        no += 1
        c = readFile(f)
        document = documents.splitDoc(c)
        size = len(document)
        if document_num + size >= 1000:
            doIndex()
            # tokenizing(document)
            # document_num += size
        else:
            document_num += size
            tokenizing(document)
    doIndex()
    cachemore()

if __name__ == '__main__':
    main()



