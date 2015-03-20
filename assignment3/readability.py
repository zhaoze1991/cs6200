import urllib
import urllib2
import urlparse
import json
from bs4 import BeautifulSoup

def clean_text(url):
    api_token = '12b3cfff676b254d146e5e19bf405e2041ae688b'
    # with open('secret.txt') as f:
    #     api_token = f.readline()
    r = urlparse.ParseResult(scheme='http',
                             netloc='www.readability.com',
                             path='/api/content/v1/parser',
                             params='',
                             query=urllib.urlencode({'url': url, 'token': api_token}),
                             fragment='')
    response = urllib2.urlopen(r.geturl()).read() # in json format
    soup = BeautifulSoup(json.loads(response)['content'])
    return soup.get_text()
