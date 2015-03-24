#!/usr/bin/env python
from elasticsearch import Elasticsearch
import uuid

index_name = 'hw3'
doc_type = 'document'


def init_index():
    client = Elasticsearch(timeout=100)

    if client.indices.exists(index_name):
        client.indices.delete(index_name)

    # create empty index
    client.indices.create(
        index=index_name,
        body={
            'settings': {
                'index': {
                    'store': {
                        'type': 'default'
                    },
                    'number_of_shards': 1,
                    'number_of_replicas': 0
                },
                "analysis": {
                    "analyzer": {
                        "my_english": {
                            "type": "english",
                            "stopwords_path": "stoplist.txt"
                        }
                    }
                }
            }
        }
    )

    client.indices.put_mapping(
        index=index_name,
        doc_type=doc_type,
        body={
            doc_type: {
                "properties": {
                    "url":{ # canonical url
                        "type": "string",
                        "store": "true",
                        "index": "not_analyzed"
                    },
                    "title": {
                        "type": "string",
                        "store": "true"
                    },
                    "html": { # raw html text
                        "type": "string",
                        "store": 'true',
                    },
                    "header": { # http header
                        "type": "string",
                        "store": "true",
                        "index": "not_analyzed"
                    },
                    "text": { # cleaned main content text of html
                        "type": "string",
                        "store": 'true',
                        "index": "analyzed",
                        "analyzer": "my_english"
                    },
                    # in-links and out-links are array type
                    # but in elastic search, there is no special mapping for array type
                    "in-links": {
                        "type": "string",
                        "store": "true",
                    },
                    "out-links": {
                        "type": "string",
                        "store": "true"
                    }
                }
            }
        }
    )


# assume in_link_list and out_link_list are lists of urls, not ids
def save_to_es(url, clean_text, raw_html, http_header, in_link_list, out_link_list):
    es = Elasticsearch()
    in_link_list = map(lambda url: uuid.uuid5(uuid.NAMESPACE_DNS, url), in_link_list)
    out_link_list = map(lambda url: uuid.uuid5(uuid.NAMESPACE_DNS, url), out_link_list)
    doc = {
        'url': url,
        'html': raw_html,
        'header': http_header,
        'text': clean_text,
        'in-links': in_link_list,
        'out-links': out_link_list
    }
    print doc
    return
    ret = es.index(index=index_name,
                   doc_type=doc_type,
                   id=uuid.uuid5(uuid.NAMESPACE_DNS, url),
                   body=doc)
    # print doc
    return ret['created']

init_index()
