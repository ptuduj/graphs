
def rec_clique_count(graph, k, nset):
    if k == 1:
        return len(nset)

    current = 0
    for neighbour in nset:
        out_nb = graph.out_neighbours(neighbour)
        common_n = nset.intersect(out_nb)

        if len(common_n) >= k-2:
            x = rec_clique_count(graph, k-1, common_n)
            current += x
    return current


def clique_count_vertex_parallel(sc, graph, k):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())

    return graph\
        .get_rows()\
        .map(lambda row: rec_clique_count(non_spark_graph.value, k-1, row.neighbours))\
        .sum()