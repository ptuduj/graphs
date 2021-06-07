from src.sets.set import Set


def rec_bron_kerbosch(R, P, X, graph):
    if len(P) == 0 and len(X) == 0:
        return [R]

    out = []
    for v in P:
        nb = graph.out_neighbours(v)
        out.extend(rec_bron_kerbosch(R.add(v), P.elems_from(v).intersect(nb), P.elems_to(v).add(v).union(X).intersect(nb), graph))
    return out


def spark_bron_kerbosh(v, R, P, X, graph):
    if len(P) == 0 and len(X) == 0:
        return [R]

    nb = graph.out_neighbours(v)
    return rec_bron_kerbosch(R.add(v), P.elems_from(v).intersect(nb), P.elems_to(v).add(v).union(X).intersect(nb), graph)


def map_bron_kerbosch(graph, x):
    v = x.vId
    nb = x.neighbours
    all_vertices_list = graph.all_nodes()
    all_vertices_set = Set.factory(nb, all_vertices_list)
    R = Set.factory(nb, [v])
    P = all_vertices_set.elems_from(v).intersect(nb)
    X = all_vertices_set.elems_to(v).intersect(nb)

    out = []
    for v in P:
        out.append((v, R, P, X))
    return out


def bron_kerbosch(sc, graph):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    return graph \
        .get_rows() \
        .flatMap(lambda x: map_bron_kerbosch(non_spark_graph.value, x))\
        .flatMap(lambda x: spark_bron_kerbosh(x[0], x[1], x[2], x[3], non_spark_graph.value))\
        .collect()