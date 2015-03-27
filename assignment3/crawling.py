#!/usr/local/bin/python
import robotparser, os
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
import myq
reload(sys)
sys.setdefaultencoding("utf-8")


# data definition
es = Elasticsearch()
f = open('storage_' + str(os.getpid()), 'w')
l = open('linkgraph_' + str(os.getpid()), 'w')
counter = 0
seeds = [
    'http://en.wikipedia.org/wiki/List_of_highest-grossing_films',
    # 'http://www.imdb.com/boxoffice/alltimegross',
    'http://en.wikipedia.org/wiki/Avatar_(2009_film)',
    'http://www.imdb.com/title/tt0499549/'
]


class Link(object):
    """docstring for Link"""
    def __init__(self, url):
        super(Link, self).__init__()
        self.url = url
        self.in_link = 1
        self.out_link = 0
        self.visited = False
        self.round = 1
        self.outs = set()
        self.ins = set()
        self.header = ''


q = myq.MyQueue()
hash_map = {}  # url -> Link,
spam_map = {}  # url -> whatever, should store this to filter un-related document
domain_time = {}  # domain -> last time visit


def check_crawler(url):
    # check if the url can be visited
    try:
        rp = robotparser.RobotFileParser()
        domain = get_domain(url)
        protocol = get_protocol(url) + '://'
        bots_location = protocol + domain
        rp.set_url(bots_location + '/robots.txt')
        rp.read()
        return rp.can_fetch("*", url)
    except:
        return False


def url_normalization(current_url, url):
    # TODO make this function better
    norm = urlnorm.norms(url)
    return norm


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
        if lapsed < 1:
            time.sleep(1 - lapsed)
            domain_time[domain] = time.time()
        else:
            domain_time[domain] = time.time()
    else:
        domain_time[domain] = time.time()
    pass


def filtered(url, content):
    if 'film' in url or 'movie' in 'url' or 'film' in content or 'movie' in content or 'imdb' in url:
        return False
    return True
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
        header = wtf.get_header(urls)
    except:
        return
    # skip not english and not html
    if header[0] != 'en' or 'text/html' not in header[1]:
        return
    soup = bs4.BeautifulSoup(response)
    title = soup.title.string.encode('utf-8')
    clean_text = wtf.clean_text(soup.findAll(text=True))
    small = clean_text.lower()
    if filtered(urls.lower(), small):
        return
    global counter
    counter += 1
    print 'dumped: ', counter
    wtf.dump_to_file(f, urls, title, header[2], clean_text, response)
    for link in soup.find_all('a'):
        link = link.get('href')
        # TODO we can filter more
        if link is None:
            continue
        temp = link.encode('utf-8')
        if len(temp) < 1 or temp[0] == '#':
            continue
        try:
            temps = url_formatter(urls, temp)
        except:
            print temps
            continue
        if temps in hash_map:
            hash_map[temps].in_link += 1
            q.update(hash_map[temps])
            if temps not in hash_map[temps].ins:
                hash_map[temps].ins.add(temps)
            if temps not in url.outs:
               url.outs.add(temps)
            continue
        if '.jpg' in temps or '.JPG' in temps or '.png' in temps or '.PNG' in temps or '.svg' in temps or '.SVG' in temps:
            continue
        hash_map[temps] = Link(temps)
        url.outs.add(temps)
        hash_map[temps].round = url.round + 1
        hash_map[temps].ins.add(urls)
        q.push(hash_map[temps])


if __name__ == '__main__':
    for seed in seeds:
        hash_map[seed] = Link(seed)
        q.push(hash_map[seed])
    global counter
    while not q.empty() and counter < 20000:
        element = q.pop()
        try:
            fetch_page(element)
        except:
            continue
        pass
    for url in hash_map:
        if len(hash_map[url].outs) < 1:
            continue
        wtf.dump_link_graph(l, url, hash_map[url].outs)
    f.close()
    l.close()


