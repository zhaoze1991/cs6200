#!/usr/bin/env python
file1_name = 'a'
file2_name = 'titanic'


class Item(object):
    """tring for Item"""
    def __init__(self):
        super(Item, self).__init__()

l1 = open(file1_name, 'r').readlines()
l2 = open(file2_name, 'r').readlines()
res = open('res','w')
def run():
    for i in range(len(l1)):
        line1 = l1[i].split()
        line2 = l2[i].split()
        val = (int(line1[3]) + int(line2[3])) / 2
        res.writelines(line1[0] + ' ' + line1[1] + ' ' + line1[2] + ' ' + str(val) + ' \n')
run()
res.close()

