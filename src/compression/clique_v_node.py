from __future__ import annotations

from src.algorithms.tomita import tomita
from src.graphs.rddGraphSet import EdgeRDDGraph,NaiveSparklessSet
from src.algorithms.bronKerbosch import bron_kerbosch
from pyspark.sql.types import *
from src.graphs.rddGraphSet import CustomRow
from pyspark.sql import functions as F
from src.sets.set import HashSet


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
        out.append((vid,el)) # against the rules vnode first
    out.sort(key=lambda x: x[0])
    return out

def edge_graph_to_v_node_edge_graph(spark,sc,rgraph,egraph,do_collect = False):
    bk = bron_kerbosch(sc,rgraph,do_collect=False).filter(lambda x: len(x) > 3).collect()
    # tom = tomita(sc, rgraph).filter(lambda x: len(x) > 3).collect()
    # print(len(bk), len(tom))

    bk.sort(key = lambda x: -len(x))
    distinct = HashSet([])
    res = []

    for el in bk:
        if (len(el.intersect(distinct)) == 0):
            distinct = distinct.union(el)
            res.append(el)
    print(len(res))

    bk = sc.parallelize(res)
    non_spark_graph = sc.broadcast(egraph.get_spark_less_copy())

    edges_to_remove = bk.flatMap(set_to_list_of_edges)
    edges_to_add = bk.zipWithIndex().flatMap(lambda x: edges_from_elemnt(
    non_spark_graph.value.max_vertice_idx() + x[1] +1,x[0]))
    n_edges_rdd = egraph.get_edges().map(lambda x: (x['id_1'],x['id_2']))
    n_edges_rdd = n_edges_rdd.subtract(edges_to_remove)
    n_edges_rdd = n_edges_rdd.union(edges_to_add)

    schema = StructType([StructField("id_1", IntegerType(), False),StructField("id_2", IntegerType(), False)])
    edges_df= spark.createDataFrame(n_edges_rdd,schema)
    edges_df = edges_df.groupBy("id_1").agg(F.collect_list("id_2").alias("neighbours"))
    custom_rows_rdd = edges_df.rdd.map(
        lambda row: CustomRow(row["id_1"], HashSet(row["neighbours"], from_sorted=False)))
    e_v_graph = EdgeRDDGraph(n_edges_rdd,custom_rows_rdd,max_vertices=non_spark_graph.value.max_vertice_idx())
    return e_v_graph


