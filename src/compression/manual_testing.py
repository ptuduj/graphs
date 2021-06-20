import os, sys

currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))

if rootDir not in sys.path: # add parent dir to paths
    sys.path.append(rootDir)

import argparse
# from src.algorithms.triangleCountEdgeParallel import triangle_count2
# from src.algorithms.CliqueCountEdgeParallel import clique_count2
from src.algorithms.tomita import tomita
from src.algorithms.bronKerbosch import bron_kerbosch
# from src.algorithms.CliqueCountVertexParallel import clique_count, rec_clique_count
from src.algorithms.KCliqueListing import rec_clique_listing, clique_listing
from src.algorithms.KCliqueStar import k_clique_star
# from src.algorithms.triangleCountVertexParallel import triangle_count, triangle_count_per_vertex
from src.graphs.rddGraphSet import CustomRow, RDDGraphSet
from src.sets.set import SortedListSet, HashSet, RoaringBitMapSet
from src.graph_creation import *
from src.benchmarks.benchmarks import Benchmarks
from src.pipeline.pipeline import Pipeline

import time

from pyspark.sql import SparkSession

from src.benchmarks.benchmarks import Benchmarks
from src.graph_creation import create_graph

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.graphs.rddGraphSet import CustomRow, RDDGraphSet, EdgeRDDGraph, GraphRepresentation
from src.compression.clique_v_node import edge_graph_to_v_node_edge_graph
from src.algorithms.triangleCountEdgeParallel import triangle_count_edge_parallel_v_graph

def main():
    spark =SparkSession.builder \
        .master("local") \
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 100)
    sc = spark.sparkContext
    # s = [HashSet([1,2,3]), HashSet([4,5]), HashSet([4,5,6,7,8,9])]
    # rdd = sc.parallelize(s)
    # a = rdd.filter(lambda x: len(x) >= 3).sortBy(lambda x: -len(x))
    # print(a.count())
    # for i in a.collect():
    #     print(i)


    filename = '../../src/graphDatasets/musae_facebook_edges.csv'
    rgraph,egraph = create_both_representations(spark, filename, HashSet, [remove_duplicate_edges, make_edges_oriented])
    vgraph = edge_graph_to_v_node_edge_graph(spark, sc, rgraph, egraph)
    print('----------------------------')
    egraph_slc_e= egraph.get_spark_less_copy().edges_count()
    egraph_slc_v = egraph.get_spark_less_copy().vertices_count()
    vgraph_slc_e = vgraph.get_spark_less_copy().edges_count()
    vgraph_slc_v = vgraph.get_spark_less_copy().vertices_count()

    print(f'egraph_edges = {egraph_slc_e} ;  egraph_verticies = {egraph_slc_v}')
    print(f'vgraph_edges = {vgraph_slc_e} ;  vgraph_verticies = {vgraph_slc_v}')
    # print(vgraph.get_spark_less_copy().edges_count())
    # print(vgraph.get_spark_less_copy().vertices_count())
    # print(triangle_count_edge_parallel_v_graph(sc,vgraph))
    pass

if __name__ == '__main__':
    main()