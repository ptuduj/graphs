
def rec_clique_count(graph, k, u, nset):
    if k == 1:
        return len(nset)

    current = 0
    for neighbrour in nset:
        common_n = nset.intersect(graph.out_neighbours(neighbrour))
        if len(common_n) >= k-2:
            x = rec_clique_count(graph, k-1, neighbrour, common_n)
            current += x
    return current


def clique_count_map(graph, k, u, v):
    nset = graph.out_neighbours(u)
    if k == 1:
        return len(nset)

    common_n = nset.intersect(graph.out_neighbours(v))
    if len(common_n) >= k-2:
        return rec_clique_count(graph, k-1, v, common_n)
    return 0



def clique_count2(sc, graph, k):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    return graph\
        .get_edges()\
        .map(lambda edge: clique_count_map(non_spark_graph.value, k-1, edge[0], edge[1]))\
        .sum()