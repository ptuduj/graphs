from src.algorithms.BronKerboschl import bron_kerboschl
from src.algorithms.CliqueCount import clique_count, rec_clique_count
from src.algorithms.KCliqueListing import rec_clique_listing, clique_listing
from src.algorithms.KCliqueStar import k_clique_star
from src.algorithms.triangleCount import triangle_count, triangle_count_per_vertex
from src.sets.set import SortedListSet, HashSet
from src.graph_creation import *



if __name__ == '__main__':

    spark = SparkSession.builder \
        .master("local") \
        .getOrCreate()
    sc = spark.sparkContext
    graph = create_undirected_graph(spark,"graphDatasets/test_graph_edges.csv",SortedListSet)

    # row0 = graph.get_edges().filter(lambda row: row.vId == 0).first()
    print("Start k-cliques listing")
    #rec_clique_listing(graph.get_spark_less_copy(), 3, row1.vId, row1.neighbours)
    l = clique_listing(sc, graph, 4)
    for elem in l:
        print(elem)

    print("Start k-cliques star")
    res = k_clique_star(sc, graph, 3)
    for elem in res:
        print(elem)

    print("k-cliques count")
    print(clique_count(sc, graph, 3))

    print('BronKerboschl')
    l = bron_kerboschl(sc, graph)
    for elem in l:
        print(elem)

    print('triangle count')
    print(triangle_count(sc, graph))


    input('enter to crash')
    spark.stop()

