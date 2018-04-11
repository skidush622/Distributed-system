#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: hash_ring.py
#
# CS6381 Assignment2
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import math
import sys
from bisect import bisect

if sys.version_info >= (2, 5):
    import hashlib
    md5_constructor = hashlib.md5
else:
    import md5
    md5_constructor = md5.new

class HashRing(object):

    def __init__(self, nodes=None, weights=None):
        """`nodes` is a list of objects that have a proper __str__ representation.
        `weights` is dictionary that sets weights to the nodes.  The default
        weight is that all nodes are equal.
        """
        self.ring = dict()
        self._sorted_keys = []

        self.nodes = nodes

        if not weights:
            weights = {}
        self.weights = weights

        self._generate_circle()

    def _generate_circle(self):
        """Generates the circle.
        """
        total_weight = 0
        for node in self.nodes:
            total_weight += self.weights.get(node, 1)

        for node in self.nodes:
            weight = 1

            if node in self.weights:
                weight = self.weights.get(node)

            #factor = math.floor((40*len(self.nodes)*weight) / total_weight);

            for j in xrange(0, 1):
                b_key = self._hash_digest( '%s-%s' % (node, j) )

                for i in xrange(0, 1):
                    key = self._hash_val(b_key, lambda x: x+i*4)
                    self.ring[key] = node
                    self._sorted_keys.append(key)

        self._sorted_keys.sort()

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        pos = self.get_node_pos(string_key)
        if pos is None:
            return None
        return self.ring[ self._sorted_keys[pos] ]

    def remove_node(self, node):
        """
        Remove the specified node and rebuild the hash ring, then return the new hashring
        """
        self.nodes.remove(node)
        self.__init__(self.nodes)

    def get_successor_table(self, node):
        '''
        get successor table for a specified table, return a node list
        '''
        successor_keys = []
        for index, key in enumerate(self._sorted_keys):
            if self.ring[key] == node:
                successor_keys = self._sorted_keys[index+1:]
                successor_keys.extend(self._sorted_keys[:index])
                break

        successor_table = []
        for key in successor_keys:
            successor_table.append(str(self.ring[key]))

        return successor_table

    def get_node_pos(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.

        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.ring:
            return None

        key = self.gen_key(string_key)

        nodes = self._sorted_keys
        pos = bisect(nodes, key)

        if pos == len(nodes):
            return 0
        else:
            return pos

    def iterate_nodes(self, string_key, distinct=True):
        """Given a string key it returns the nodes as a generator that can hold the key.

        The generator iterates one time through the ring
        starting at the correct position.

        if `distinct` is set, then the nodes returned will be unique,
        i.e. no virtual copies will be returned.
        """
        if not self.ring:
            yield None, None

        returned_values = set()
        def distinct_filter(value):
            if str(value) not in returned_values:
                returned_values.add(str(value))
                return value

        pos = self.get_node_pos(string_key)
        for key in self._sorted_keys[pos:]:
            val = distinct_filter(self.ring[key])
            if val:
                yield val

        for i, key in enumerate(self._sorted_keys):
            if i < pos:
                val = distinct_filter(self.ring[key])
                if val:
                    yield val

    def gen_key(self, key):
        """Given a string key it returns a long value,
        this long value represents a place on the hash ring.

        md5 is currently used because it mixes well.
        """
        b_key = self._hash_digest(key)
        return self._hash_val(b_key, lambda x: x)

    def _hash_val(self, b_key, entry_fn):
        return (( b_key[entry_fn(3)] << 24)
                |(b_key[entry_fn(2)] << 16)
                |(b_key[entry_fn(1)] << 8)
                | b_key[entry_fn(0)] )

    def _hash_digest(self, key):
        m = md5_constructor()
        m.update(key)
        return map(ord, m.digest())
