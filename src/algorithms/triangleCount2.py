def triangle_count_per_vertex2(graph, x):
    u = x[0]
    v = x[1]
    nb_u = graph.out_neighbours(u)
    nb_v = graph.out_neighbours(v)
    return len(nb_u.intersect(nb_v))


def triangle_count2(sc, graph):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    return graph \
        .get_edges() \
        .map(lambda edge: triangle_count_per_vertex2(non_spark_graph.value, edge))\
        .sum()