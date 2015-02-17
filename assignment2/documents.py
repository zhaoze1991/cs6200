#!/usr/local/bin/python

def handleTemp(temp):
    # extract content from <TEXT> and void <TEXT>content</TEXT>
    start = temp.find('<TEXT>') + len('<TEXT>')
    end = temp.find('</TEXT>')
    return temp[start:end]

def handleDocument(content):
    documentid = ''
    Text = ''
    i = 0
    length = len(content)
    while i < length:
        if '<DOCNO>' in content[i]:
            no = content[i].split(' ')
            documentid = no[1]
            break
            # find the doc id, and to avoid un-necessary if-check, end this loop
        i += 1
    while i < length:
        if '<TEXT>' in content[i]:
            temp = ''
            while '</TEXT>' not in content[i]:
                # replace the '\n' with space
                temp += content[i][:-1] + ' '
                i += 1
            temp += content[i]
            Text += handleTemp(temp)
        i += 1
    return (documentid, Text)

# split the file into document
def splitDoc(content):
    length = len(content)
    i = 0
    documents = []
    while i < length:
        if '<DOC>' in content[i]:
            # start to get the whole doc
            doc = []
            doc.append(content[i])
            i += 1
            while '</DOC>' not in content[i]:
                doc.append(content[i])
                i += 1
            doc.append(content[i])
            documents.append(handleDocument(doc))
        i += 1
    return documents
