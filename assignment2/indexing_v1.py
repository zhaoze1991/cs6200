#!/usr/local/bin/python
import os, sys, re, collections, threading, glob, snowballstemmer, documents

stop_list = {}
space = ' '


class IndexEntry(object):
    """docstring for IndexEntry, we will have four IndexEntry"""
    def __init__(self, name):
        super(IndexEntry, self).__init__()
        self.name = name
        self.hash_map = {}  # term id, df, ttf, and storage block
        self.df = {}  # term id -> frequency
        self.ttf = {}
        self.v = {}  # unique term
        self.doc_len = {}  # document id -> length
        self.term_id = {}  # term -> id
        self.id_term = {}  # id -> term
        self.id = 0
        self.documents = {}
        self.document_id = 0

    def clean(self):
        self.hash_map = {}
        self.df = {}
        self.ttf = {}
    pass


def tokenizing(entry, docs, stems, stop):
    # no stem, no stop word
    for d in docs:
        tf = {}
        words = {}
        position = 1
        doc_no = d[0]
        entry.documents[doc_no] = entry.document_id
        content = d[1]
        pattern = re.compile('\w+(?:\.?\w+)*')
        tokens = pattern.findall(content)
        for t in tokens:
            t = t.lower()
            i = 0
            # skip first _
            while i < len(t) and t[i] == '_':
                i += 1
            t = t[i:]
            if t == '':
                continue
            if stems:
                t = stem(t)
            if stop:
                if t in stop_list:
                    continue
            if t in entry.term_id:
                pass
            else:
                entry.term_id[t] = entry.id
                entry.id_term[entry.id] = t
                entry.id += 1
            if t in words:
                words[t].append(position)
                tf[t] += 1
            else:
                words[t] = [position]
                tf[t] = 1
            position += 1
        for w in words:
            term_id = entry.term_id[w]
            # unique term
            if term_id in entry.ttf:
                entry.ttf[term_id] += tf[w]
            else:
                entry.ttf[term_id] = tf[w]
            if term_id in entry.v:
                pass
            else:
                entry.v[term_id] = True
            if term_id in entry.df:
                entry.df[term_id] += 1
            else:
                entry.df[term_id] = 1
            if term_id in entry.hash_map:
                tuples = (tf[w], ) + tuple(words[w])
                entry.hash_map[term_id][entry.document_id] = tuples
            else:
                tuples = (tf[w],) + tuple(words[w])
                entry.hash_map[term_id] = {}
                entry.hash_map[term_id][entry.document_id] = tuples
        entry.doc_len[entry.document_id] = position
        entry.document_id += 1


def stem(word):
    # static variable
    stem.stemmer = snowballstemmer.stemmer('english')
    word = stem.stemmer.stemWord(word)
    return word


def load_stop_list():
    global stop_list
    stop = open('stoplist.txt', 'r').readlines()
    for word in stop:
        stop_list[word[:-1]] = True


def load_file_name():
    path = './AP_DATA/ap89_collection/*'
    files = []
    for f in glob.glob(path):
        files.append(f)
    # remove readme from the files
    del files[-1]
    return files


def write_file(entry, name):
    result = sorted(entry.hash_map.items(), key=lambda x: x[0])
    cache = open(name, 'w')
    category = open(name + '_category', 'w')
    start = 0
    for block in result:
        term = block[0]
        string = str(term) + space + str(entry.df[term]) + space + str(entry.ttf[term]) + space
        for b in block[1]:
            string += str(b) + space
            for i in block[1][b]:
                string += str(i) + space
        # erase the last space
        string = string[:-1]
        string += '\n'
        cache.write(string)
        cate = str(term) + space + str(start) + space + str(len(string)) + '\n'
        category.write(cate)
        start += len(string)
    cache.close()
    category.close()


def indexing(entry):
    name = 'cache1_' + entry.name
    if not os.path.exists(name):
        write_file(entry, name)
        return
    name = 'cache2_' + entry.name
    if not os.path.exists(name):
        write_file(entry, name)
        documents.mergefile(entry.name)
    pass


def dump_info(entry):
    name = 'info_' + entry.name
    f = open(name, 'w')
    for _id in entry.documents:
        f.write(str(entry.documents[_id]) + space + _id + '\n')
    f.close()


def main():
    # clean the cached files
    os.system('./clean.sh')
    load_stop_list()
    files = load_file_name()  # return all the file name
    document_number = 0
    ff = IndexEntry('ff')
    ft = IndexEntry('ft')
    tf = IndexEntry('tf')
    tt = IndexEntry('tt')
    count = 0
    for f in files:
        content = documents.read_file(f)  # return the content by the file name
        document = documents.splitDoc(content)  # split the document to doc_no and content
        count += 1
        print count
        size = len(document)
        if document_number + size >= 1000:
            indexing(ff)
            indexing(ft)
            indexing(tf)
            indexing(tt)
            ff.clean()
            ft.clean()
            tf.clean()
            tt.clean()
            # sys.exit(-1)
            document_number = size
            tokenizing(ff, document, False, False)
            tokenizing(ft, document, False, True)
            tokenizing(tf, document, True, False)
            tokenizing(tt, document, True, True)
        else:
            tokenizing(ff, document, False, False)
            tokenizing(ft, document, False, True)
            tokenizing(tf, document, True, False)
            tokenizing(tt, document, True, True)
            document_number += size
    indexing(ff)
    indexing(ft)
    indexing(tf)
    indexing(tt)
    dump_info(ff)
    dump_info(ft)
    dump_info(tf)
    dump_info(tt)
    pass


if __name__ == '__main__':
    main()
