import os
from tempfile import mkstemp

from parallelm.common.buff_to_lines import BufferToLines
from parallelm.common.topological_sort import TopologicalSort


TMP_FILE_CONTENT = b"""
123 abc
4567 de fg

"""


class TestCommon:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_buff_2_lines(self):
        _, tmp_file = mkstemp(prefix='test_mlcomp_common', dir='/tmp')

        with open(tmp_file, "wb") as f:
            f.write(TMP_FILE_CONTENT)

        try:
            buff2lines = BufferToLines()
            with open(tmp_file, 'rb') as f:
                content = f.read()
                assert content == TMP_FILE_CONTENT
                buff2lines.add(content)

            raw_lines = TMP_FILE_CONTENT.decode().split('\n')
            for index, line in enumerate(buff2lines.lines()):
                assert line == raw_lines[index]

        finally:
            os.remove(tmp_file)

    def test_topological_sort(self):
        class Node(object):
            def __init__(self, key, childs):
                self._key = key
                self._childs = childs if isinstance(childs, list) else [childs]

            @property
            def key(self):
                return self._key

            @property
            def childs(self):
                return self._childs

            def __str__(self):
                child_keys = [c.key for c in self.childs if c] if self.childs else None
                return "key: {}, childs: {}".format(self.key, child_keys)

        n1 = Node("a", None)
        n2 = Node("b", n1)
        n3 = Node("c", [n1])
        n4 = Node("d", [n2, n3])
        n5 = Node("e", [n3])

        graph_list = [n3, n1, n2, n4, n5]
        sorted_graph1 = TopologicalSort(graph_list, "key", "childs").sort()
        print("Graph1:")
        for node in sorted_graph1:
            print(node)

        self._validate_sorted_graph(sorted_graph1, n1, n2, n3, n4, n5)

        graph_dict = {n3.key: n3, n1.key: n1, n2.key: n2, n4.key: n4, n5.key: n5}
        sorted_graph2 = TopologicalSort(graph_dict, "key", "childs").sort()
        print("\nGraph2:")
        for node in sorted_graph2:
            print(node)

        self._validate_sorted_graph(sorted_graph2, n1, n2, n3, n4, n5)

    def _validate_sorted_graph(self, sorted_graph, n1, n2, n3 , n4, n5):
        assert len(sorted_graph) == 5, "Unexpected number of nodes in topological sorted graph! " \
                                       "expected: 5, existing: {}".format(len(sorted_graph))

        assert sorted_graph[0] == n1, \
            "The first node should be node n1['a']! sorted: {}".format(self._graph_str(sorted_graph))

        index_n1 = sorted_graph.index(n1)
        index_n2 = sorted_graph.index(n2)
        index_n3 = sorted_graph.index(n3)
        index_n4 = sorted_graph.index(n4)
        index_n5 = sorted_graph.index(n5)

        assert index_n1 < index_n2, \
            "Node n1['a'] is expected to be before n2['b']! sorted: {} " \
            .format(self._graph_str(sorted_graph))

        assert index_n1 < index_n3, \
            "Node n3['b'] is expected to be after n1['a']! sorted: {} " \
            .format(self._graph_str(sorted_graph))

        assert index_n2 < index_n4 and index_n3 < index_n4, \
            "Node n4['d'] is expected to be after n2['b'] and n3['c']! sorted: {} " \
            .format(self._graph_str(sorted_graph))

        assert index_n3 < index_n5, \
            "Node n5['e'] is expected to be after n3['c']! sorted: {} " \
            .format(self._graph_str(sorted_graph))

    def _graph_str(self, graph):
        return " <= ".join([str(n) for n in graph])
