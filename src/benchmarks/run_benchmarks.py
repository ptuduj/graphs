import os, sys

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
from src.sets.set import SortedListSet, HashSet, RoaringBitMapSet
from src.graph_creation import *
from src.benchmarks.benchmarks import Benchmarks
from src.pipeline.pipeline import Pipeline



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="f_command_line")
    parser.add_argument('-c',default=False,action='store_true')
    args = parser.parse_args()

    if args.c :
        file_name = "src/graphDatasets/musae_git_edges.csv"
    else:
        file_name = '../graphDatasets/musae_git_edges.csv'

    # preprocessing_for_directed_graph = [remove_self_edges, make_edges_oriented, remove_duplicate_edges]
    preprocessing_for_undirected_graph = [remove_self_edges, make_graph_undirected]

    # pipeline = Pipeline(file_name, HashSet, {triangle_count2: [], clique_count2: [3]},
    #                     [Benchmarks.TimeCount, Benchmarks.ProcessedEdgesPerSec],
    #                     GraphRepresentation.EdgeRDDGraph, preprocessing_for_directed_graph)

    pipeline = Pipeline(file_name, HashSet,
                        {tomita: []},
                         #    , tomita: [], triangle_count: [],
                         # k_clique_star: [3], clique_count: [3]},
                        [Benchmarks.TimeCount, Benchmarks.ProcessedVerticesPerSec],
                        GraphRepresentation.RDDGraphSet, preprocessing_for_undirected_graph)

    # pipeline.create_graph()
    # print("v1", pipeline.graph.vertices_count())
    # print("v2", pipeline.graph._rdd_custom_rows.count())

    res = pipeline.run_all()
    print(res)



    input('enter to crash')