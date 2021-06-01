from src.algorithms.BronKerboschl import bron_kerboschl
from src.algorithms.CliqueCount import clique_count, rec_clique_count
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
    # edge_list_graph = create_edgeListGraph(spark, "graphDatasets/musae_git_edges.csv", HashSet)
    # print("xxxx ", edge_list_graph.get_edges().count())
    custom_rows_graph = create_rddGraphSet(spark, "graphDatasets/musae_git_edges.csv", HashSet)
    # print('triangle count')
    # t_start = time.time()
    # res = triangle_count2(sc, edge_list_graph)
    # t = time.time() - t_start
    # print("Time ", t, " s")
    # print(res)

    # t_start = time.time()
    # res = triangle_count(sc, custom_rows_graph)
    # t = time.time() - t_start
    # print("Time ", t, " s")
    # print(res)


    # row0 = graph.get_edges().filter(lambda row: row.vId == 0).first()
    # print("Start k-cliques listing")
    # #rec_clique_listing(graph.get_spark_less_copy(), 3, row1.vId, row1.neighbours)
    # l = clique_listing(sc, graph, 4)
    # for elem in l:
    #     print(elem)
    #
    # print("Start k-cliques star")
    # res = k_clique_star(sc, graph, 3)
    # for elem in res:
    #     print(elem)
    #
    print("k-cliques count")
    print(clique_count(sc, custom_rows_graph, 3))
    #
    # print('BronKerboschl')
    # l = bron_kerboschl(sc, graph)
    # for elem in l:
    #     print(elem)



    input('enter to crash')
    spark.stop()

