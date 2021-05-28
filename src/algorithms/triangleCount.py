def triangle_count_per_vertex(graph, x):
    u = x.vId
    nb_u = graph.out_neighbours(u)
    total = 0
    for v in nb_u:
        if u < v:
            total += len(nb_u.intersect(graph.out_neighbours(v)))
    return total


def triangle_count(sc, graph):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    count = graph \
        .get_rows() \
        .map(lambda row: triangle_count_per_vertex(non_spark_graph.value, row))\
        .sum()

    assert(count % 3 == 0)
    return count // 3