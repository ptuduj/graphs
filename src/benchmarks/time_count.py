import time
import numpy as np
import os, sys
from pyspark.sql import SparkSession


currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path: # add parent dir to paths
    sys.path.append(rootDir)

import argparse
from src.algorithms.triangleCount2 import triangle_count2
from src.algorithms.CliqueCount2 import clique_count2
from src.algorithms.tomita import tomita
from src.algorithms.bronKerbosch import bron_kerbosch

from src.algorithms.CliqueCount import clique_count, rec_clique_count
from src.algorithms.KCliqueListing import rec_clique_listing, clique_listing
from src.algorithms.KCliqueStar import k_clique_star
from src.algorithms.triangleCount import triangle_count, triangle_count_per_vertex
from src.graphs.rddGraphSet import CustomRow, RDDGraphSet
from src.sets.set import SortedListSet, HashSet
from src.graph_creation import *
from src.util.printing import *
import pickle


RETRIES = 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="f_command_line")
    parser.add_argument('-c',default=False,action='store_true')
    args = parser.parse_args()
    tested_graph_file_path = None
    if args.c :
        file_name = "src/graphDatasets/Slashdot0811.csv"
    else:
        file_name = '../graphDatasets/Slashdot0811.csv'
    spark = SparkSession.builder \
        .master("local") \
        .getOrCreate()
    sc = spark.sparkContext

    preprocessing_for_directed_graph = [remove_self_edges, make_edges_oriented, remove_duplicate_edges]
    preprocessing_for_undirected_graph = [make_graph_undirected]

    directed_edgeList_graph = create_graph(spark, file_name, HashSet, GraphRepresentation.EdgeListGraphSet, preprocessing_for_directed_graph)
    # undirected_rdd_graph = create_graph(spark, file_name, HashSet, GraphRepresentation.RDDGraphSet, preprocessing_for_undirected_graph)
    # directed_rdd_graph = create_graph(spark, file_name, HashSet, GraphRepresentation.RDDGraphSet, preprocessing_for_directed_graph)

    t_start = time.time()
    l = triangle_count2(sc, directed_edgeList_graph)
    t = time.time() - t_start
    print("Time ", t, " s")
    print(l)

    input('enter to crash')
    spark.stop()