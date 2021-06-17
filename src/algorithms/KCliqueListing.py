from src.sets.set import Set


def rec_clique_listing(R, P, X, graph, k):
    if k == 1:
        return [R]

    if len(P) == 0 and len(X) == 0:
        return [R]

    out = []
    for v in P:
        nb = graph.out_neighbours(v)
        out.extend(rec_clique_listing(
                        R.add(v),
                        P.elems_from(v).intersect(nb),
                        P.elems_to(v).add(v).union(X).intersect(nb),
                        graph,
                        k - 1))
    return out


def spark_clique_listing(graph, x, k):
    v = x.vId
    nb = x.neighbours
    all_vertices_list = graph.all_nodes()
    all_vertices_set = Set.factory(nb, all_vertices_list)
    R = Set.factory(nb, [v])
    P = all_vertices_set.elems_from(v).intersect(nb)
    X = all_vertices_set.elems_to(v).intersect(nb)
    return rec_clique_listing(R, P, X, graph, k)


def clique_listing(sc, graph, k):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    return graph \
        .get_rows() \
        .flatMap(lambda row: spark_clique_listing(non_spark_graph.value, row, k)) \
        .filter(lambda clique: len(clique) == k) \
        .collect()