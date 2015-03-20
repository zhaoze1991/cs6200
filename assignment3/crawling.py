#!/usr/local/bin/python
import robotparser
import bs4
import urlparse
import urlnorm
import collections
import urllib2
import time
import re
import httplib
from HTMLParser import HTMLParser
import readability
import wtf
import socket
from elasticsearch import Elasticsearch
import uuid
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

es = Elasticsearch()

def dump_to_es(url, html, clean_text, header):
    doc = {
    'url':url,
    'html':html,
    'text':clean_text,
    'header':header
    }
    es.index(index = 'hw3',
        doc_type = 'document',
        id = uuid.uuid5(uuid.NAMESPACE_URL, url),
        body =doc
        )
    # print 'index'
    pass

counter = 0
seeds = [
    'http://en.wikipedia.org/wiki/List_of_highest-grossing_films',
    # 'http://www.imdb.com/boxoffice/alltimegross',
    'http://en.wikipedia.org/wiki/Avatar_(2009_film)',
    'http://www.imdb.com/title/tt0499549/'
]

f = open('storage', 'w')

class Link(object):
    """docstring for Link"""
    def __init__(self, url):
        super(Link, self).__init__()
        self.url = url
        self.in_link = 1
        self.out_link = 0
        self.visited = False
        self.round = 1
        self.outs = []
        self.ins = []
        self.header = ''


class IndexItem(object):
    def __init__(self, url):
        super(IndexItem, self).__init__()
        # we will use the url as the id
        self.url = url
        self.clean_text = ''
        self.raw_html = ''
        self.in_link = []
        self.out_link = []


q = wtf.MyQueue()
hash_map = {}  # url -> Link,
spam_map = {}  # url -> whatever, should store this to filter un-related document
domain_time = {}  # domain -> last time visit


def get_header(urls):
    # return language and content type
    req = urllib2.Request(urls)
    # req.get_method = lambda: 'HEAD'
    information = urllib2.urlopen(req).info()
    return [information.getheader('content-language'), information.getheader('content-type'), information]


def check_crawler(url):
    # check if the url can be visited
    rp = robotparser.RobotFileParser()
    domain = get_domain(url)
    protocol = get_protocol(url) + '://'
    bots_location = protocol + domain
    rp.set_url(bots_location + '/robots.txt')
    rp.read()
    return rp.can_fetch("*", url)


def url_normalization(current_url, url):
    # TODO make this function better
    norm = urlnorm.norms(url)
    return norm


def write_to_file(docno, title, content):
    f.writelines('<DOC>\n')
    f.writelines('<DOCNO> ' + docno + ' </DOCNO>\n')
    if title is not None:
        # todo may be there's bug
        f.writelines('<HEAD> ' + title + ' </HEAD>\n')
    f.writelines('<TEXT>\n')
    f.writelines(content)
    f.writelines('</TEXT>\n')
    f.writelines('</DOC>\n')


def get_protocol(url):
    return urlparse.urlparse(url).scheme
    pass


def get_domain(url):
    return urlparse.urlparse(url).hostname
    pass


def url_formatter(urls, link):
    # this function is used to format the link that we get from urls
    if link[:2] == '//':
        return url_normalization(urls, get_protocol(urls) + ':' + link)
    if link[0] == '/':
        current = get_protocol(urls) + '://' + get_domain(urls) + link
        return url_normalization(urls, current)
    return url_normalization(urls, link)
    pass


def sleep_function(url):
    domain = get_domain(url)
    if domain in domain_time:
        lapsed = time.time() - domain_time[domain]
        if lapsed < 1.0:
            time.sleep(1.0 - lapsed)
            domain_time[domain] = time.time()
        else:
            domain_time[domain] = time.time()
    else:
        domain_time[domain] = time.time()
    pass


def fetch_page(url):
    # todo change the check sequence
    urls = url.url
    print urls, url.in_link
    sleep_function(urls)
    if not check_crawler(urls):
        return
    try:
        response = urllib2.urlopen(urls).read() # this is the raw html
    except:
        return
    header = wtf.get_header(urls)
    if header[0] != 'en' or 'text/html' not in header[1]:
        return
    soup = bs4.BeautifulSoup(response)
    title = soup.title.string.encode('utf-8')
    clean_text = wtf.clean_text(soup.findAll(text=True))
    try:
        clean_text = readability.clean_text(urls).encode('utf-8')
    except:
        return
    # clean_text_low = clean_text.lower()
    # print clean_text_low
    # if 'film' not in clean_text_low and 'movie' not in clean_text_low:
    #     spam_map[urls] = True
    #     return
    global counter
    counter += 1
    print counter
    dump_to_es(urls, response, clean_text, header[2])
    # write_to_file(urls, title, clean_text)
    for link in soup.find_all('a'):
        link = link.get('href')
        # TODO we can filter more
        if link is None:
            continue
        temp = link.encode('utf-8')
        if len(temp) < 1:
            continue
        if temp[0] == '#':
            continue
        temps = url_formatter(urls, temp)
        if temps in hash_map:
            hash_map[temps].in_link += 1
            hash_map[temps].ins.append(urls)
            continue
        if '.jpg' in temps or '.JPG' in temps or '.png' in temps or '.PNG' in temps:
            continue
        if '.svg' in temps or '.SVG' in temps:
            continue
        hash_map[temps] = Link(temps)
        url.outs.append(temps)
        hash_map[temps].round = url.round + 1
        hash_map[temps].ins.append(urls)
        q.push(hash_map[temps])


def filter_mechanism(url):
    if url in spam_map:
        return True
    temp_url = url.lower()
    if 'film' in temp_url or 'movie' in temp_url:
        return False
    try:
        sleep_function(url)
        header = get_header(url)
        if header[0] != 'en' or 'text/html' not in header[1]:
            spam_map[url] = True
            return True
    except:
        spam_map[url] = True
        return True
    spam_map[url] = True
    return True
    pass


def update_es(url, ins, outs):
    inns = ''
    ouuts = ''
    for i in ins:
        inns += i + ' '
    for i in outs:
        ouuts += i + ' '
    doc = {
        'doc':{
            'in-links': inns,
            'out-links' : ouuts
        }
    }
    # print doc
    es.update(index = 'hw3',
        id = str(uuid.uuid5(uuid.NAMESPACE_URL, url)),
        doc_type = 'document',
        body = doc
        )
    pass


if __name__ == '__main__':
    link_map = open('links', 'w')
    global counter
    for seed in seeds:
        hash_map[seed] = Link(seed)
        q.push_seeds(hash_map[seed])
    while not q.empty() and counter < 5:
        element = q.pop()
        fetch_page(element)
        pass
    for url in hash_map:
        if len(hash_map[url].outs) < 1:
            continue
        link_map.writelines(url + ' ')
        for links in hash_map[url].outs:
            link_map.writelines(links + ' ')
        link_map.writelines('\n')



