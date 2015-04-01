#!/usr/bin/env python

outlinks_file = open('link_graph_ly.txt', 'r')
inlinks_file = open('link_graph_ly2.txt', 'w')
inlinks = {}  # url -> list of inlinks in url

for line in outlinks_file.readlines():
    urls = line.split()
    if (len(urls)) < 1:
        continue
    url = urls[0]
    inlinks[url] = []

outlinks_file.seek(0)

for line in outlinks_file.readlines():
    urls = line.split()
    if (len(urls)) < 1:
        continue
    url = urls[0]
    outlinks = urls[1:]
    for ol in outlinks:
        if ol in inlinks:
            inlinks[ol].append(url)

# Write to file
for url in inlinks:
    inlinks_file.write(url + ' ' + ' '.join(inlinks[url]) + '\n')

inlinks_file.flush()
inlinks_file.close()
