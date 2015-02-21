#!/usr/local/bin/python
import os, sys
space = ' '


# read file by filename


def read_file(filename):
    f = open(filename, 'r')
    content = f.readlines()
    f.close()
    return content

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

def getNumber(content):
    index = 0
    # first number, second number, and the position to be continue
    result = []
    size = len(content)
    while index < size:
        if content[index] == ' ':
            temp = ''
            index += 1
            while index < size and content[index] != ' ':
                temp += content[index]
                index += 1
            result.append(int(temp))
        else:
            index += 1
        if len(result) == 2:
            while index < size and content[index] == ' ':
                index += 1
            result.append(index)
            return result

def mergefile(name):
    # data file
    print 'mergefile'
    file1 = open('cache1_' + name,'r')
    file2 = open('cache2_' + name,'r')
    file3 = open('cache3_' + name, 'w')
    cate3 = open('cache3_' + name + '_category', 'w')
    # category file
    cate1 = open('cache1_' + name + '_category', 'r').readlines()
    cate2 = open('cache2_' + name + '_category', 'r').readlines()
    ptr1 = 0
    ptr2 = 0
    start = 0
    while ptr1 < len(cate1) and ptr2 < len(cate2):
        # line in category
        # term start len
        line1 = cate1[ptr1].split(' ')
        line2 = cate2[ptr2].split(' ')
        t1 = line1[0]
        t2 = line2[0]
        result = ''
        if t1 < t2:
            result += t1 + space + str(start) + space
            file1.seek(int(line1[1]))
            content = file1.read(int(line1[2][:-1]))
            file3.write(content)
            size = len(content)
            result += str(size) + '\n'
            cate3.write(result)
            start += size
            ptr1 += 1
        elif t1 > t2:
            result += t2 + space + str(start) + space
            file2.seek(int(line2[1]))
            content = file2.read(int(line2[2][:-1]))
            file3.write(content)
            size = len(content)
            result += str(size) + '\n'
            cate3.write(result)
            start += size
            ptr2 += 1
        elif t1 == t2:
            # if two terms are the same
            result += t1 + space + str(start) + space
            file1.seek(int(line1[1]))
            content1 = file1.read(int(line1[2][:-1]))
            space1 = getNumber(content1)
            file2.seek(int(line2[1]))
            content2 = file2.read(int(line2[2][:-1]))
            # print content2
            space2 = getNumber(content2)
            data = t1 + space + str(space1[0] + space2[0]) + space
            data += str(space1[1] + space2[1]) + space + content1[space1[2]:-1]
            data += space + content2[space2[2]:]
            file3.write(data)
            size = len(data)
            result += str(size) + '\n'
            cate3.write(result)
            start += size
            ptr1 += 1
            ptr2 += 1
    while ptr1 < len(cate1):
        line1 = cate1[ptr1].split(' ')
        t1 = line1[0]
        result = t1 + space + str(start) + space
        file1.seek(int(line1[1]))
        content = file1.read(int(line1[2][:-1]))
        file3.write(content)
        size = len(content)
        result += str(size) + '\n'
        cate3.write(result)
        start += size
        ptr1 += 1
    while ptr2 < len(cate2):
        line2 = cate2[ptr2].split(' ')
        t2 = line2[0]
        result = t2 + space + str(start) + space
        file2.seek(int(line2[1]))
        content = file2.read(int(line2[2][:-1]))
        file3.write(content)
        size = len(content)
        result += str(size) + '\n'
        cate3.write(result)
        start += size
        ptr2 += 1
    file1.close()
    file2.close()
    file3.close()
    cate3.close()
    os.remove('cache1_' + name)
    os.remove('cache2_' + name)
    os.remove('cache1_' + name + '_category')
    os.remove('cache2_' + name + '_category')
    os.rename('cache3_' + name, 'cache1_' + name)
    os.rename('cache3_' + name + '_category', 'cache1_' + name + '_category')
