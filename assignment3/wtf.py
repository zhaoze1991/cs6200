#!/usr/bin/env python
from collections import deque
import collections

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


