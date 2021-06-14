from src.algorithms.KCliqueListing import clique_listing


def clique_star(graph, clique):
    s = None
    for i, vertex in enumerate(clique):
        nb = graph.out_neighbours(vertex)
        if i == 0:
            s = nb
        s = s.intersect(nb)

    #print("s:", s, clique)
    if s.is_empty():
        return []

    s = s.union(clique)
    return s


def k_clique_star(sc, graph, k):
    k_cliques = clique_listing(sc, graph, k)
    rdd_k_cliques = sc.parallelize(k_cliques)
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())

    return rdd_k_cliques \
        .map(lambda clique: clique_star(non_spark_graph.value, clique)) \
        .filter(lambda star: star != []) \
        .distinct() \
        .collect()
