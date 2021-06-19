from __future__ import annotations
from src.graphs.rddGraphSet import EdgeRDDGraph,NaiveSparklessSet
from src.algorithms.bronKerbosch import bron_kerbosch
from pyspark.sql.types import *
from src.graphs.rddGraphSet import CustomRow
from pyspark.sql import functions as F
from src.sets.set import HashSet

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
        # out.append((el,vid))
        out.append((vid,el)) # against the rules vnode first
    out.sort(key=lambda x: x[0])
    return out

def edge_graph_to_v_node_edge_graph(spark,sc,rgraph,egraph,do_collect = True):
    bk = bron_kerbosch(sc,rgraph,do_collect=False).filter(lambda x: len(x) > 3)
    non_spark_graph = sc.broadcast(egraph.get_spark_less_copy())
    # print('nlist')
    # print(non_spark_graph.value._n_list)
    # print('oryginal_edges')
    # print(egraph.get_edges().map(lambda x: (x['id_1'],x['id_2'])).collect())
    edges_to_remove = bk.flatMap(set_to_list_of_edges)
    # print('edges to remove')
    # print(edges_to_remove.collect())
    edges_to_add = bk.zipWithIndex().flatMap(lambda x: edges_from_elemnt(
    non_spark_graph.value.max_vertice_idx() + x[1] +1,x[0]))
    # print('edges_to_add')
    # print(edges_to_add.collect())
    n_edges_rdd = egraph.get_edges().map(lambda x: (x['id_1'],x['id_2'])).subtract(edges_to_remove).union(edges_to_add)
    if do_collect:
        n_edges = n_edges_rdd.collect()
        print('final edges')
        for edge in n_edges:
            print(edge)
        # print('hue')
        # return
    schema = StructType([StructField("id_1", IntegerType(), False),StructField("id_2", IntegerType(), False)])
    edges_df= spark.createDataFrame(n_edges_rdd,schema)
    edges_df.show()
    edges_df = edges_df.groupBy("id_1").agg(F.collect_list("id_2").alias("neighbours"))
    edges_df.show()
    custom_rows_rdd = edges_df.rdd.map(
        lambda row: CustomRow(row["id_1"], HashSet(row["neighbours"], from_sorted=False)))
    print('custom_rows_rdd after map' )
    custom_rows_rdd_c = custom_rows_rdd.collect()
    for el in custom_rows_rdd_c:
        print(el)
    e_v_graph = EdgeRDDGraph(n_edges_rdd,custom_rows_rdd,max_vertices=non_spark_graph.value.max_vertice_idx())
    print('show v_Graph')
    for el in e_v_graph._rdd_custom_rows.collect():
        print(el)
    print('e_V_graph_n_list')
    print(e_v_graph._n_list)
    print('e_v_graph_sparkless_graph_n_list')
    print(e_v_graph.get_spark_less_copy()._n_list)
    print('org graph max node nr, and then sparklesscopy')
    print(e_v_graph._max_vertice_idx)
    print(e_v_graph.get_spark_less_copy()._max_vertice_idx)
    return e_v_graph

def graph_from_edge_list_list():
    pass

def main():
    s={0,1,3,15,2}
    print(set_to_list_of_edges(s))
    pass


if __name__ == '__main__':
    main()
