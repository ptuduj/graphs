import os, sys


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



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="f_command_line")
    parser.add_argument('-c',default=False,action='store_true')
    args = parser.parse_args()


    if args.c :
        file_name = "src/graphDatasets/test_graph_edges.csv"
    else:
        file_name = '../graphDatasets/musae_git_edges.csv'

    preprocessing_list1 = [remove_self_edges, make_edges_oriented, remove_duplicate_edges]
    preprocessing_list2 = [remove_self_edges, make_graph_undirected]

    pipeline = Pipeline(file_name, HashSet, {triangle_count_edge_parallel: [], clique_count_edge_parallel: [4]},
                        [Benchmarks.TimeCount, Benchmarks.ProcessedEdgesPerSec],
                        GraphRepresentation.EdgeRDDGraph, preprocessing_list1)

    # pipeline = Pipeline(file_name, RoaringBitMapSet,
    #                     {
    #                         tomita: [],
    #                         #bron_kerbosch: []
    #                      },
    #                     [Benchmarks.TimeCount, Benchmarks.ProcessedVerticesPerSec],
    #                     GraphRepresentation.RDDGraphSet, preprocessing_list2)

    # pipeline = Pipeline(file_name, HashSet,
    #                     {
    #                         triangle_count_vertex_parallel: [],
    #                         clique_count_vertex_parallel: [3]
    #                      },
    #                     [Benchmarks.TimeCount, Benchmarks.ProcessedVerticesPerSec],
    #                     GraphRepresentation.RDDGraphSet, preprocessing_list1)

    res = pipeline.run_all()
    for elem in res:
        print(elem)



    input('enter to crash')