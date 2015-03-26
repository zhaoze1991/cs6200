#!/usr/bin/env python
import os
from collections import deque
import collections
import urllib2
import requests
import uuid
import re
import bs4


def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element)):
        return False
    return True

# text = soup.findAll(text=True)
def clean_text(text):
    visible_text = filter(visible, text)
    texts = ''
    for item in visible_text:
        texts += item.encode('utf-8')
    return texts


def url_to_uuid(url):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, url))
    pass

class MyQueue(object):
    """docstring for MyQueue"""
    def __init__(self):
        super(MyQueue, self).__init__()
        self.seeds = collections.deque()
        self.queue = collections.deque()

    def push_seeds(self, item):
        self.seeds.append(item)

    def push(self, item):
        self.queue.append(item)

    def empty(self):
        if len(self.seeds) == 0 and len(self.queue) == 0:
            return True
        return False

    def seed_empty(self):
        return len(self.seeds) == 0

    def pop(self):
        if len(self.seeds) > 0:
            self.seeds = collections.deque(sorted(self.seeds, key = lambda x:-x.in_link))
            return self.seeds.popleft()
        self.queue = collections.deque(sorted(self.queue, key = lambda x:-x.in_link))
        element = self.queue.popleft()
        in_link = element.in_link
        if len(self.queue) > 0:
            next_element = self.queue.popleft()
            if next_element.in_link == in_link:
                temp_list = [element, next_element]
                while next_element.in_link == in_link and len(self.queue) > 0:
                     next_element = self.queue.popleft()
                     temp_list.append(next_element)
                temp_list = sorted(temp_list, key = lambda x : x.round)
                res = temp_list[0]
                temp_list.remove(res)
                self.queue.extendleft(temp_list)
                return res
            else:
                self.queue.appendleft(next_element)
        return element

def get_header(url):
    resp = requests.head(url).headers
    res = []
    if 'content-language' in resp:
        res.append(resp['content-language'])
    else:
        res.append('en')
    res.append(resp['content-type'])
    res.append(str(resp))
    return res

def dump_to_file(f, docno, title, head, text, raw):
    # f is the file object
    f.writelines('<DOC>\n')
    f.writelines('<DOCNO> ' + docno + ' </DOCNO>\n')
    if title is not None:
        f.writelines('<HEAD> ' + title + ' </HEAD>\n')
    if head is not None:
        # this is the html header
        f.writelines('<HTML-HEAD>\n' + head + '\n</HTML-HEAD>\n')
    f.writelines('<TEXT> \n' + text + '</TEXT>\n')
    f.writelines('<RAW> \n' + raw + '\n</RAW>\n')
    f.writelines('</DOC>\n')

def dump_link_graph(f, url, outlinks):
    f.writelines(url + ' ')
    f.writelines(' '.join(outlinks))
    f.writelines('\n')
    pass

def check_exist(es, url):
    # es the the elastic search object
    return es.exists(index = 'hw3',
              id = wtf.url_to_uuid(url),
              doc_type = 'document'
              )


