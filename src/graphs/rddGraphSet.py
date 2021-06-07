from src.sets.set import SortedListSet, HashSet
from enum import Enum


class GraphType(Enum):
    DIRECTED = 0
    UNDIRECTED = 1


class GraphRepresentation(Enum):
    RDDGraphSet = 0
    EdgeListGraphSet = 1
    

class NaiveSparklessSet:
    def __init__(self, n_list):
        self._n_list = n_list.copy()

    def all_nodes(self):
        return [i for i in range(len(self._n_list))]

    def out_neighbours(self, vid):
        if vid in self._n_list:
            return self._n_list[vid]
        else:
            return HashSet([])

    def get_vertex_num(self):
        return len(self._n_list)


class CustomRow:
    def __init__(self, vId, neighbours):
        self.vId = vId
        self.neighbours = neighbours


class RDDGraphSet:
    def __init__(self, rdd_custom_rows):
        self._rdd_custom_rows = rdd_custom_rows
        rows = rdd_custom_rows.collect()
        self._n_list = {r.vId: r.neighbours for r in rows}

    def all_nodes(self):
        nodes_count = self._rdd_custom_rows.count()
        return [i for i in range(nodes_count)]

    def get_rows(self):
        return self._rdd_custom_rows

    def out_degree(self, vertexID):
        return len(self.out_neighbours(vertexID))

    def out_neighbours(self, vertex_ID):
        return self._n_list[vertex_ID]

    def get_spark_less_copy(self):
        return NaiveSparklessSet(self._n_list)


class EdgeListGraphSet:
    def __init__(self, edge_list_rdd, rdd_custom_rows):
        self.edge_list_rdd = edge_list_rdd
        self._rdd_custom_rows = rdd_custom_rows
        rows = rdd_custom_rows.collect()
        self._n_list = {r.vId: r.neighbours for r in rows}

    def all_nodes(self):
        nodes_count = self._rdd_custom_rows.count()
        return [i for i in range(nodes_count)]

    def get_edges(self):
        return self.edge_list_rdd

    def out_degree(self, vertexID):
        return len(self.out_neighbours(vertexID))

    def out_neighbours(self, vertex_ID):
        return self._n_list[vertex_ID]

    def get_spark_less_copy(self):
        return NaiveSparklessSet(self._n_list)
