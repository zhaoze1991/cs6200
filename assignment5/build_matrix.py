#!/usr/local/bin/python
import documents, indexing, collections, wtf, time
import sys, os
from itertools import islice
stop_list = {}
term_list = []
recent_search = {}
all_docs = {}  # qid -> {docno -> label}
matrix_hash = {} # qid -> my_matrix object

K = 100


def filter_document():
    f = open('./AP_DATA/qrels.adhoc.51-100.AP89.txt', 'r').readlines()
    for line in f:
        line = line.split()
        qid, label = int(line[0]), int(line[3])
        if qid not in all_docs:
            all_docs[qid] = {}
        all_docs[qid][line[2]] = label


class MyMatrix(object):
    def __init__(self, qid):
        self.qid = qid
        self.tf_idf, self.bm25, self.okapi = {}, {}, {}
        self.jel, self.lap, self.label = {}, {}, {} # docid -> corresponding score
        self.category, self.prox = '', {}
    pass


def load_stop_list():
    global stop_list
    stop = open('stoplist.txt', 'r').readlines()
    for word in stop:
        stop_list[word[:-1]] = True


class Document(object):
    """docstring for Document"""
    def __init__(self, the_id, info):
        super(Document, self).__init__()
        self.id = the_id
        self.information = info


class IndexEntry(object):
    """docstring for IndexEntry"""
    def __init__(self, arg, stem, stop):
        super(IndexEntry, self).__init__()
        self.name = arg
        self.v = 0
        self.document = {}  # document id, -> document no, length
        self.category = {}  # term, -> start, length of the term
        self.average = 0
        self.stem = stem
        self.stop = stop
        self.D = 0
        self.tdl = 0  # total document length


class Term(object):
    def __init__(self, arg):
        super(Term, self).__init__()
        self.value = arg
        self.document = {}  # document id -> tf
        self.df = 0
        self.ttf = 0
        self.position = {}  # document id -> positions

def build_matrix_to_file():
    global matrix_hash
    f = open('feature_matirx', 'w')
    for qid in matrix_hash:
        obj = matrix_hash[qid]
        for doc in all_docs[qid]:
            res = str(qid) + ' ' + doc + ' '
            res += str(obj.tf_idf[doc]) + ' ' + str(obj.bm25[doc]) + ' ' + str(obj.okapi[doc]) + ' '
            res += str(obj.jel[doc]) + ' ' + str(obj.lap[doc]) + ' '
            try:
                res += str(obj.prox[doc]) + ' ' + str(obj.label[doc]) + ' '
            except:
                res +=  ' 0 ' + str(obj.label[doc]) + ' '
            res += obj.category + '\n'
            f.writelines(res)
    f.close()


def dump_position(entry, term, docs):
    cache = open('cache1_' + entry.name, 'r')
    cate = entry.category[term.value]
    cache.seek(cate[0])
    content = cache.read(cate[1]).split(' ')
    index = 2
    while index < len(content):
        doc_id = int(content[index])
        if doc_id in docs:
            docs[doc_id] += 1
        else:
            docs[doc_id] = 1
        index += 1
        tf = int(content[index])
        index += 1
        positions = []
        last = index + tf
        while index < last:
            positions.append(int(content[index]))
            index += 1
        term.position[doc_id] = positions
        pass
    cache.close()
    return docs


def dump_cache(entry, term):
    cache = open('cache1_' + entry.name, 'r')
    cate = entry.category[term.value]
    cache.seek(cate[0])
    content = cache.read(cate[1]).split(' ')
    term.df = int(content[0])
    term.ttf = int(content[1])
    # print term.df, term.ttf
    index = 2
    while index < len(content):
        doc_id = int(content[index])
        index += 1
        tf = int(content[index])
        index += 1
        term.document[doc_id] = tf
        index += tf


def dump_info(entry):
    info = open('info_' + entry.name, 'r').readlines()
    index = 0
    tdl = 0
    while index < len(info) - 2:
        line = info[index].split(' ')
        tdl += int(line[2])
        entry.document[int(line[0])] = [line[1], int(line[2])]
        index += 1
    entry.v = int(info[index])
    index += 1
    entry.average = float(info[index])
    entry.D = len(entry.document)
    entry.tdl += tdl


def dump_category(entry):
    cate = open('cache1_' + entry.name + '_category').readlines()
    for line in cate:
        line = line.split(' ')
        entry.category[line[0]] = [int(line[1]), int(line[2])]


def string_operation(q, stem):
    global stop_list
    query = q[q.find('Document'):len(q)-1]
    i = len(query) - 1
    last = len(query) - 1
    while i > 0:
        if not query[i].isalpha():
            i -= 1
            continue
        last = i
        break
    query = query[0:last+1]
    query = query.split(' ')
    length = len(query)
    query = query[3:length]
    # skip stop words
    result = []
    for item in query:
        item = item.lower()
        if item == '' or item == ' ' or item == ',':
            continue
        if item in stop_list:
            continue
        if item[-1:].isalpha() == False and item[:-1]+'\n' in stop_list:
            continue
        if stem:
            item = indexing.stem(item)
        result.append(item)
    return result


def sort(data, k):
    # sort the dictionary, and return the first k pairs
    result = sorted(data.items(), key=lambda x:(-x[1]))
    return list(islice(result, k))


def sort_v2(data, k):
    kk = len(data)
    if kk > k:
        kk = k
    result = sorted(data.items(), key=lambda x:(-x[1]))
    return list(islice(result, kk))

def get_questions(stem):
    file_content = documents.read_file('./AP_DATA/query_desc.51-100.short_manual.txt')
    questions = []
    for q in file_content:
        if 'Document' not in q:
            continue
        num = q.split(' ')[0]
        num = num[0:len(num)-1]
        # get the query
        query = string_operation(q, stem)
        q = {}
        q[num] = query
        questions.append(q)
        # questions[num]=query
    return questions


def get_postings(entry, keyword):
    output = []
    term = Term(keyword)
    dump_cache(entry, term)
    for doc_id in term.document:
        doc_info = entry.document[doc_id]
        temp = {
            'document_length': doc_info[1],
            'docno': doc_info[0],
            'tf': term.document[doc_id],
            'df': term.df,
            'ttf': term.ttf
        }
        d = Document(doc_id, temp)
        output.append(d)
    return output
    # pass


def term_frequency_in_query(term, query):
    freq = 0
    for t in query:
        if t == term:
            freq += 1
    return freq


def search(entry,qid, query, k, category):
    Okapi_TF_scores = collections.defaultdict(lambda: 0.0)
    TF_IDF_scores = collections.defaultdict(lambda: 0.0)
    Okapi_BM25_socres = collections.defaultdict(lambda: 0.0)
    Laplace_scores = collections.defaultdict(lambda: 0.0)
    Jelinek_Mercer_scores = collections.defaultdict(lambda: 0.0)
    # this search will iterate every item in query
    for item in query:
        posting = get_postings(entry, item)
        # term frequency in query
        freq = term_frequency_in_query(item, query)
        hit = {}
        ttf = 0
        for doc in posting:
            docno = doc.information['docno']
            if docno not in all_docs[qid]:
                continue
            hit[doc.id] = 1
            Okapi_TF_scores[docno] += wtf.Okapi_TF(doc, entry.average)
            TF_IDF_scores[docno] += wtf.TF_IDF(doc, entry.average, entry.D)
            Okapi_BM25_socres[docno] += wtf.Okapi_BM25(freq, doc, entry.average, entry.D)
            Laplace_scores[docno] += wtf.Laplace(doc, entry.v)
            ttf = float(doc.information['ttf'])
            Jelinek_Mercer_scores[docno] += wtf.Jelinek_Mercer(doc, ttf, entry.tdl)
        for doc in entry.document:
            if doc in hit:
                continue
            docno = entry.document[doc][0]
            if docno not in all_docs[qid]:
                continue
            Laplace_scores[docno] += wtf.Laplace2(0, int(entry.document[doc][1]), int(entry.v))
            try:
                Jelinek_Mercer_scores[docno] += wtf.Jelinek_Mercer2(ttf, entry.tdl)
            except:
                Jelinek_Mercer_scores[docno] += 0
    global matrix_hash
    obj = MyMatrix(qid)
    for doc in all_docs[qid]:
        obj.tf_idf[doc], obj.bm25[doc] = TF_IDF_scores[doc], Okapi_BM25_socres[doc]
        obj.okapi[doc], obj.jel[doc] = Okapi_TF_scores[doc], Jelinek_Mercer_scores[doc]
        obj.lap[doc], obj.label[doc] = Laplace_scores[doc], all_docs[qid][doc]
    obj.category = category
    matrix_hash[qid] = obj
    Okapi_TF_scores = sort(Okapi_TF_scores, k)
    TF_IDF_scores = sort(TF_IDF_scores, k)
    Okapi_BM25_socres = sort(Okapi_BM25_socres, k)
    Laplace_scores = sort(Laplace_scores, k)
    Jelinek_Mercer_scores = sort(Jelinek_Mercer_scores, k)
    return [Okapi_TF_scores, TF_IDF_scores, Okapi_BM25_socres, Laplace_scores,
    Jelinek_Mercer_scores]


def write_file(container, filename):
    # this function is used to write result to the file
    f = open(filename, 'w')
    for content in container:
        f.writelines(content)
    f.close()


def output_format(container, result, temp):
    # this function is used to give the right output format
    i = 1
    for r in result:
        temp_ = temp
        temp_ += r[0] + ' ' + str(i) + ' ' + str(r[1]) + ' Exp\n'
        container.append(temp_)
        i += 1
    return container


def new_search(entry, qid, questions, K, category):
    proximity = collections.defaultdict(lambda: 0.0)
    item_list, docs, q, res = [], {}, {}, []
    # docs = collections.defaultdict(lambda: 0)
    C = 1500
    for qq in questions:
        if qq in q:
            continue
        res.append(qq)
        q[qq] = True
    for item in res:
        it = Term(item)
        dump_position(entry, it, docs)
        item_list.append(it)
    for doc in docs:
        if docs[doc] < 2:
            continue
        k = 0
        matrix = []
        s = 0
        for it in item_list:
            if doc in it.position:
                k += 1
                matrix.append(it.position[doc])
        s = 1
        if k != 1:
            s = documents.get_min_span(matrix)
        # up = float(s - k) / k
        # proximity[entry.document[doc][0]] = 0.8 ** up
        score = float((C - s) * docs[doc])
        score /= (entry.document[doc][1] + entry.v)
        proximity[entry.document[doc][0]] = score
    obj = matrix_hash[qid]
    for item in proximity:
        if item not in all_docs[qid]:
            continue
        obj.prox[item] = proximity[item]
    proximity = sort_v2(proximity, K)
    return [proximity]
    pass


def do_search(entry):
    q = get_questions(entry.stem)
    Okapi_TF = []
    TF_IDF = []
    Okapi_BM25 = []
    Laplace_smoothing = []
    Jelinek_Mercer = []
    proximity, count = [], 0
    for questions in q:
        count += 1
        category = 'train'
        if count > 20:
            category = 'test'
        for index in questions:
            print 'search ', questions[index]
            temp = ''
            temp += str(index) + ' Q0 '
            result = search(entry, int(index), questions[index], K, category)
            Okapi_TF = output_format(Okapi_TF, result[0], temp)
            TF_IDF = output_format(TF_IDF, result[1], temp)
            Okapi_BM25 = output_format(Okapi_BM25, result[2], temp)
            Laplace_smoothing = output_format(Laplace_smoothing, result[3], temp)
            Jelinek_Mercer = output_format(Jelinek_Mercer, result[4], temp)
            new_model = new_search(entry, int(index), questions[index], K, category)
            proximity = output_format(proximity, new_model[0], temp)
    pass


def new_main(name, stem, stop):
    entry = IndexEntry(name, stem, stop)
    dump_info(entry)
    dump_category(entry)
    do_search(entry)
    build_matrix_to_file()


if __name__ == '__main__':
    load_stop_list()
    filter_document()
    new_main('tt', True, True)
