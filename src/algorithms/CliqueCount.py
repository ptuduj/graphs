from math import factorial

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


def clique_count(sc, graph, k):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())

    count = graph\
        .get_edges()\
        .map(lambda x: rec_clique_count(non_spark_graph.value, k-1, x.vId, x.neighbours))\
        .sum()

    assert(count % factorial(k) == 0)
    return count // factorial(k)
