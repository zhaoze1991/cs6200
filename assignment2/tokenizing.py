#!/usr/local/bin/python
import os, sys, re, collections, struct, thread
import glob
import snowballstemmer
import documents
# data definition
# ------------------------------------------------------------
files = []
path = './AP_DATA/ap89_collection/*'
# id -> term
id_term = {}
termid = 0
# term -> [(docno, position)]
hash_map = {}
hash_map_stem = {}
space = ' '
cf = {}
df = {}
vf = {}
document_id = 0
document_num = 0
stoplist = {}

def loadstoplist():
    global stoplist
    stop = open('stoplist.txt','r').readlines()
    for word in stop:
        stoplist[word[:-1]] = 1

def getFileName():
    global files
    for file in glob.glob(path):
        files.append(file)
    # remove readme from the file
    del files[-1]



def stem(word):
    # static variable
    stem.stemmer = snowballstemmer.stemmer('english')
    word = stem.stemmer.stemWord(word)
    return word

def token_non(documents):
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


def writefile(fileName, content, stem, stop):
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

def indexing(stem, stop):
    result = sort()
    if os.path.exists('cache1_F_F') == False:
        fileName = ''
        if stem == False and stop == False:
            fileName = 'cache1_F_F'
        elif stem == False and stop == True:
            fileName = 'cache1_F_T'
        elif stem == True and stop == False:
            fileName = 'cache1_T_F'
        elif stem == True and stop == True:
            fileName = 'cache1_T_T'
        writefile(fileName, result)
        print 'write cache1'
        return
    if os.path.exists('cache2_F_F') == False:
        fileName = ''
        if stem == False and stop == False:
            fileName = 'cache2_F_F'
        elif stem == False and stop == True:
            fileName = 'cache2_F_T'
        elif stem == True and stop == False:
            fileName = 'cache2_T_F'
        elif stem == True and stop == True:
            fileName = 'cache2_T_T'
        writefile(fileName, result)
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
    thread.start_new_thread(indexing, (False, False))
    thread.start_new_thread(indexing, (False, True))
    thread.start_new_thread(indexing, (True, False))
    thread.start_new_thread(indexing, (True, True))
    global document_num
    global hash_map
    global df
    global cf
    hash_map = {}
    df = {}
    cf = {}
    document_num = 0

def main():
    os.system('./clean.sh')
    loadstoplist()
    getFileName()
    global document_num
    for f in files:
        c = documents.read_file(f)
        document = documents.splitDoc(c)
        size = len(document)
        if document_num + size >= 1000:
            doIndex()
            tokenizing(document)
            document_num += size
        else:
            document_num += size
            tokenizing(document)
    doIndex()
    cachemore()

if __name__ == '__main__':
    main()



