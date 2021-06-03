from src.sets.set import Set

def choose_pivot(P, X, graph):
    S = P.union(X)
    curr_max = 0
    pivot_neigh = None
    for u in S:
        neigh = graph.out_neighbours(u)
        l = len(neigh.intersect(P))
        if l >= curr_max:
            curr_max = l
            pivot_neigh = neigh
    return pivot_neigh



def rec_bron_kerboschl(R, P, X, graph):
    if len(P) == 0 and len(X) == 0:
        return [R]

    out = []
    for v in P.difference(choose_pivot(P, X, graph)):
        nb = graph.out_neighbours(v)
        out.extend(rec_bron_kerboschl(R.add(v),P.intersect(nb),X.intersect(nb),graph))
        P = P.remove(v)
        X = X.add(v)
    return out


def spark_bron_kerboschl(graph, x):
    v = x.vId
    nb = x.neighbours
    all_vertices_list = graph.all_nodes()
    all_vertices_set = Set.factory(nb, all_vertices_list)
    R = Set.factory(nb, [v])
    P = all_vertices_set.elems_from(v)
    X = all_vertices_set.elems_to(v)
    nb = x.neighbours
    return rec_bron_kerboschl(R, P.intersect(nb), X.intersect(nb), graph)


def bron_kerboschl(sc, graph):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    return graph \
        .get_rows() \
        .flatMap(lambda x: spark_bron_kerboschl(non_spark_graph.value, x))\
        .collect()