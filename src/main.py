from pyspark.sql import SparkSession

from src.algorithms.BronKerbosch2 import bron_kerboschl2
from src.algorithms.BronKerboschl import bron_kerboschl
from src.algorithms.CliqueCount import clique_count, rec_clique_count
from src.algorithms.CliqueCount2 import clique_count2
from src.algorithms.KCliqueListing import rec_clique_listing, clique_listing
from src.algorithms.KCliqueStar import k_clique_star
from src.algorithms.triangleCount import triangle_count, triangle_count_per_vertex
from src.algorithms.triangleCount2 import triangle_count2
from src.sets.set import SortedListSet, HashSet
from src.graph_creation import *
import time

if __name__ == '__main__':

    spark = SparkSession.builder \
        .master("local") \
        .getOrCreate()
    sc = spark.sparkContext

    file_name = "graphDatasets/musae_git_edges.csv"

    #directed_edgeList_graph = create_graph(spark, file_name, HashSet, GraphRepresentation.EdgeListGraphSet, GraphType.DIRECTED)
    undirected_rdd_graph = create_graph(spark, file_name, HashSet, GraphRepresentation.RDDGraphSet, GraphType.UNDIRECTED)
    #directed_rdd_graph = create_graph(spark, file_name, HashSet, GraphRepresentation.RDDGraphSet, GraphType.DIRECTED)


    # print('triangle count')
    # t_start = time.time()
    # res = triangle_count2(sc, directed_edgeList_graph)
    # t = time.time() - t_start
    # print("Time ", t, " s")
    # print(res)
    #
    # t_start = time.time()
    # res = triangle_count(sc, directed_rdd_graph)
    # t = time.time() - t_start
    # print("Time ", t, " s")
    # print(res)


    # row0 = graph.get_edges().filter(lambda row: row.vId == 0).first()
    # print("Start k-cliques listing")
    # #rec_clique_listing(graph.get_spark_less_copy(), 3, row1.vId, row1.neighbours)
    # l = clique_listing(sc, undirected_rdd_graph, 4)
    # for elem in l:
    #     print(elem)

    # print("Start k-cliques star")
    # res = k_clique_star(sc, undirected_rdd_graph, 3)
    # for elem in res:
    #     print(elem)
    #
    # print("k-cliques count")
    # t_start = time.time()
    # res = clique_count(sc, directed_rdd_graph, 3)
    # t = time.time() - t_start
    # print("Time ", t, " s")
    # print(res)
    #
    # t_start = time.time()
    # res = clique_count2(sc, directed_edgeList_graph, 3)
    # t = time.time() - t_start
    # print("Time ", t, " s")
    # print(res)


    print('BronKerboschl')
    t_start = time.time()
    l = bron_kerboschl(sc, undirected_rdd_graph)
    t = time.time() - t_start
    print("Time ", t, " s")

    t_start = time.time()
    l = bron_kerboschl2(sc, undirected_rdd_graph)
    t = time.time() - t_start
    print("Time ", t, " s")

    input('enter to crash')
    spark.stop()

