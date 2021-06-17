from __future__ import annotations
from src.graphs.rddGraphSet import EdgeRDDGraph,NaiveSparklessSet
from src.algorithms.bronKerbosch import bron_kerbosch

def RDD_graph_set_to_edge_graph_set(sc,edge):

    pass

def check_if_sorted(l):
    if len(l) <2 :
        return True
    lv = l[0]
    for el in l:
        if el < lv:
            return False
    return True

def set_to_list_of_edges(s):
    l = list(s)
    if not check_if_sorted(l):
        l.sort()
    out = []
    for i in range(len(l)-1):
        for j in range(i+1,len(l)):
            out.append((l[i],l[j]))
    return out

def edges_from_elemnt(vid,nb):
    out = []
    for el in nb:
        out.append((el,vid))
    out.sort(key=lambda x: x[0])
    return out

def edge_graph_to_v_node_edge_graph(sc,rgraph,egraph):
    bk = bron_kerbosch(sc,rgraph,do_collect=False).filter(lambda x: len(x) > 3)
    print('bk')
    print(bk.collect())
    non_spark_graph = sc.broadcast(egraph.get_spark_less_copy())
    print('nlist')
    print(non_spark_graph.value._n_list)
    print('oryginal_edges')
    print(egraph.get_edges().map(lambda x: (x['id_1'],x['id_2'])).collect())
    edges_to_remove = bk.flatMap(set_to_list_of_edges)
    print('edges to remove')
    print(edges_to_remove.collect())
    edges_to_add = bk.zipWithIndex().flatMap(lambda x: edges_from_elemnt(
    non_spark_graph.value.max_vertice_idx() + x[1] +1,x[0]))
    print('edges_to_add')
    print(edges_to_add.collect())
    n_edges = egraph.get_edges().map(lambda x: (x['id_1'],x['id_2'])).subtract(edges_to_remove).union(edges_to_add).collect()

    print('final edges')
    for edge in n_edges:
        print(edge)


def main():
    s={0,1,3,15,2}
    print(set_to_list_of_edges(s))
    pass


if __name__ == '__main__':
    main()
