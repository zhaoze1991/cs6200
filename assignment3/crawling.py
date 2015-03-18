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

seeds = [
    'http://en.wikipedia.org/wiki/List_of_highest-grossing_films',
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


class IndexItem(object):
    def __init__(self, url):
        super(IndexItem, self).__init__()
        # we will use the url as the id
        self.url = url
        self.clean_text = ''
        self.raw_html = ''
        self.in_link = []
        self.out_link = []


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

q = wtf.MyQueue()
hash_map = {}  # url -> Link,


def get_header(urls):
    # return language and content type
    req = urllib2.Request(urls)
    req.get_method = lambda: 'HEAD'
    information = urllib2.urlopen(req).info()
    return [information.getheader('content-language'), information.getheader('content-type')]


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
    if len(title) != 0:
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


def fetch_page(pre_domain, url):
    # todo change the check sequence
    urls = url.url
    print urls, url.in_link,
    if pre_domain == urlparse.urlparse(urls).hostname:
        time.sleep(1)
    if not check_crawler(urls):
        return
    header = get_header(urls)
    if header[0] != 'en' or 'text/html' not in header[1]:
        return
    response = urllib2.urlopen(urls).read()  # this is the raw html
    soup = bs4.BeautifulSoup(response)
    links = []
    title = soup.title.string
    clean_text = readability.clean_text(urls).encode('utf-8')
    write_to_file(urls, title, clean_text)
    for link in soup.find_all('a'):
        link = link.get('href')
        # TODO we can filter more
        if link is None:
            continue
        temp = str(link)
        if temp[0] == '#':
            continue
        temps = url_formatter(urls, temp)
        if '.jpg' in temps or '.JPG' in temps or '.png' in temps or '.PNG' in temps:
            continue
        links.append(temps)
    for link in links:
        if link in hash_map:
            hash_map[link].in_link += 1
            continue
        hash_map[link] = Link(link)
        hash_map[link].round = url.round + 1
        q.push(hash_map[link])


if __name__ == '__main__':
    for seed in seeds:
        hash_map[seed] = Link(seed)
        q.push_seeds(hash_map[seed])
    pre_domain = ''
    while not q.empty():
        element = q.pop()
        fetch_page(pre_domain, element)
        pre_domain = get_domain(element.url)
        pass



print get_header('http://david.choffnes.com/classes/cs4700sp15/papers/tcp-sim.pdf')
