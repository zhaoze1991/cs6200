#!/usr/bin/env python

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

class MyQueue(object):
    """docstring for MyQueue"""
    def __init__(self):
        super(MyQueue, self).__init__()
        self.list = [Link('null')]  # node 0 always not used
        self.index = 0  # the index of the last element
        self.hash_map = {}  # url -> index

    def empty(self):
        # check if the priority queue is empty
        return len(self.list) == 1

    def push(self, item):
        # add item to the list
        self.list.append(item)
        self.index += 1
        self.hash_map[item.url] = self.index
        self.refresh(self.index)

    def pop(self):
        # this function will get the highest priority element in the queue
        # if self.empty(): we should check, but I have check in the crawler
            # return
        res = self.list[1]
        del self.hash_map[res.url]
        if self.index == 1:
            del self.list[-1]
            self.index -= 1
            return res
        self.list[1] = self.list[self.index]
        self.hash_map[self.list[1].url] = 1
        del self.list[-1]
        self.index -= 1
        self.heapify(1)
        return res

    def update(self, item):
        if item.url not in self.hash_map:
            return
        self.refresh(self.hash_map[item.url])

    def heapify(self, kk):
        k = kk
        while 2 * k < self.index:
            child = 2 * k
            # next_elem = object
            if self.compare(self.list[child], self.list[child + 1]):
                # next_elem = self.list[child]
                if self.compare(self.list[k], self.list[child]):
                    return
                else:
                    self.swap(k, child)
                    k = child
            else:
                # next_elem = self.list[child + 1]
                if self.compare(self.list[k], self.list[child + 1]):
                    return
                else:
                    self.swap(k, child + 1)
                    k = child + 1
        if 2 * k == self.index:
            if not self.compare(self.list[k], self.list[2 * k]):
                self.swap(k, 2 * k)


    def refresh(self, k):
        while k > 1 and self.compare(self.list[k], self.list[k / 2]):
            self.swap(k, k / 2)
            k = k / 2
            pass

    def swap(self, index1, index2):
        temp = self.list[index1]
        self.list[index1] = self.list[index2]
        self.list[index2] = temp
        self.hash_map[self.list[index1].url] = index1
        self.hash_map[self.list[index2].url] = index2

    def compare(self, item1, item2):
        # return if item1 has higher priority
        if item1.round == 1:  # which means seed
            if item2.round == 1:
                if item1.in_link > item2.in_link:
                    return True
                else:
                    return False
            else:
                return True
        elif item2.round == 1:
            return False
        if item1.in_link > item2.in_link:
            return True
        elif item1.in_link < item2.in_link:
            return False
        elif item1.in_link == item2.in_link:
            if item1.round < item2.round:
                return True
            else:
                return False


