import os, sys

from pyspark.sql import SparkSession

currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))

if rootDir not in sys.path: # add parent dir to paths
    sys.path.append(rootDir)

import argparse
from src.algorithms.tomita import tomita
from src.algorithms.bronKerbosch import bron_kerbosch
from src.algorithms.KCliqueListing import rec_clique_listing, clique_listing
from src.algorithms.KCliqueStar import k_clique_star
from src.graphs.rddGraphSet import CustomRow, RDDGraphSet
from src.sets.set import SortedListSet, HashSet, RoaringBitMapSet
from src.graph_creation import *
from src.benchmarks.benchmarks import Benchmarks
from src.pipeline.pipeline import Pipeline
from src.algorithms.CliqueCountVertexParallel import clique_count_vertex_parallel
from src.algorithms.triangleCountVertexParallel import triangle_count_vertex_parallel
from src.algorithms.CliqueCountEdgeParallel import clique_count_edge_parallel
from src.algorithms.triangleCountEdgeParallel import triangle_count_edge_parallel
import pickle


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="f_command_line")
    parser.add_argument('-c',default=False,action='store_true')
    args = parser.parse_args()


    if args.c :
        file_name = "src/graphDatasets/roadNet-CA.csv"
    else:
        file_name = '../graphDatasets/roadNet-CA.csv'

    spark = SparkSession.builder \
            .master("local") \
        .getOrCreate()
    sc = spark.sparkContext

    preprocessing_list2 = [make_graph_undirected]
    graph = create_graph(spark, file_name, HashSet, GraphRepresentation.RDDGraphSet, preprocessing_list2)
    remove_paths(sc, graph)
    # print(triangle_count_vertex_parallel(sc, graph) /3)
    # print("here", triangle_count_vertex_parallel(sc, compressed_graph) /3)




    # s1 = graph.get_spark_less_copy()
    # pickle.dump(s1, open("h", "wb"))
    #
    # graph2 = create_graph(spark, file_name, RoaringBitMapSet, GraphRepresentation.RDDGraphSet, [])
    # s2 = graph2.get_spark_less_copy()
    # pickle.dump(s2, open("h_r", "wb"))


    # preprocessing_list1 = [remove_self_edges, remove_duplicate_edges, make_edges_oriented]
    # preprocessing_list2 = [remove_self_edges, make_graph_undirected]
    # pipeline = Pipeline(file_name, HashSet, {
    #     triangle_count_edge_parallel: [], clique_count_edge_parallel: [4]} ,
    #                     [Benchmarks.TimeCount],
    #                     GraphRepresentation.EdgeRDDGraph, preprocessing_list1)
    #
    # pipeline = Pipeline(file_name, RoaringBitMapSet,
    #                     {
    #                         tomita: [],
    #                         bron_kerbosch: []
    #                      },
    #                     [Benchmarks.TimeCount, Benchmarks.ProcessedVerticesPerSec],
    #                     GraphRepresentation.RDDGraphSet, preprocessing_list2)
    #
    # pipeline = Pipeline(file_name, HashSet,
    #                     {
    #                         triangle_count_vertex_parallel: [],
    #                         clique_count_vertex_parallel: [4]
    #                      },
    #                     [Benchmarks.TimeCount, Benchmarks.ProcessedVerticesPerSec],
    #                     GraphRepresentation.RDDGraphSet, preprocessing_list1)
    #
    # res = pipeline.run_all()
    # for elem in res:
    #     print(elem)


    input('enter to crash')