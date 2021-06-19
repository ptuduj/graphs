from src.sets.set import SortedListSet, HashSet, RoaringBitMapSet
from enum import Enum


class GraphRepresentation(Enum):
    RDDGraphSet = 0
    EdgeRDDGraph = 1
    VirtualNodesGraph = 2
    

class NaiveSparklessSet:
    def __init__(self, n_list, _all_vertices, _vertices_count, _edges_count,_max_vertice_idx):
        self._n_list = n_list.copy()
        self._all_vertices = _all_vertices
        self._vertices_count = _vertices_count
        self._edges_count = _edges_count
        self._max_vertice_idx= _max_vertice_idx

    def edges_count(self):
        return self._edges_count


    def vertices_count(self):
        return self._vertices_count


    def all_nodes(self):
        return self._all_vertices

    def out_neighbours(self, vid):
        if vid in self._n_list:
            return self._n_list[vid]
        else:
            return HashSet([])

    def get_vertex_num(self):
        return len(self._n_list)

    def max_vertice_idx(self):
        return self._max_vertice_idx

    def is_virtual(self,vid):
        if vid> self._max_vertice_idx:
            return True
        else:
            return False

class CustomRow:
    def __init__(self, vId, neighbours):
        self.vId = vId
        self.neighbours = neighbours



class RDDGraphSet:
    def __init__(self, rdd_custom_rows):
        self._rdd_custom_rows = rdd_custom_rows
        rows = rdd_custom_rows.collect()
        self._n_list = {r.vId: r.neighbours for r in rows}
        self._all_vertices = list(set([edge for sublist in self._n_list.values() for edge in sublist] + list(self._n_list.keys())))
        self._max_vertice_idx = max(self._all_vertices)
        self._vertices_count = len(self._all_vertices)
        self._edges_count = len([edge for sublist in self._n_list.values() for edge in sublist])

    def edges_count(self):
        return self._edges_count


    def vertices_count(self):
        return self._vertices_count


    def all_nodes(self):
        return self._all_vertices


    def get_rows(self):
        return self._rdd_custom_rows

    def out_degree(self, vertexID):
        return len(self.out_neighbours(vertexID))

    def out_neighbours(self, vertex_ID):
        return self._n_list[vertex_ID]

    def get_spark_less_copy(self):
        return NaiveSparklessSet(self._n_list, self._all_vertices, self._vertices_count, self._edges_count,self._max_vertice_idx)


class EdgeRDDGraph:
    def __init__(self, edge_list_rdd, rdd_custom_rows,max_vertices=None):
        self.edge_list_rdd = edge_list_rdd
        self._rdd_custom_rows = rdd_custom_rows
        rows = rdd_custom_rows.collect()
        self._n_list = {r.vId: r.neighbours for r in rows}
        self._all_vertices = list(set([edge for sublist in self._n_list.values() for edge in sublist] + list(self._n_list.keys())))
        self._vertices_count = len(self._all_vertices)
        self._max_vertice_idx = max(self._all_vertices) if max_vertices is None else max_vertices
        self._edges_count = len([edge for sublist in self._n_list.values() for edge in sublist])

    def edges_count(self):
        return self._edges_count


    def vertices_count(self):
        return self._vertices_count


    def all_nodes(self):
        return self._all_vertices

    def get_edges(self):
        return self.edge_list_rdd

    def out_degree(self, vertexID):
        return len(self.out_neighbours(vertexID))

    def out_neighbours(self, vertex_ID):
        return self._n_list[vertex_ID]

    def get_spark_less_copy(self):
        return NaiveSparklessSet(self._n_list, self._all_vertices, self._vertices_count, self._edges_count,self._max_vertice_idx)
