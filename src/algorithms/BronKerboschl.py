from src.sets.sortedListSet import SortedListSet

def recBronKerboschl(R,P,X,graph):
    if len(P) == 0 and len(X) == 0:
        return [R]
    out = []
    for v in P:
        nb = graph.out_neighbours(v)
        out.extend(recBronKerboschl(R.add(v),P.elems_from(v).intersect(nb),X.intersect(nb),graph))
        # P = P.remove(v)
        X = X.add(v)
    return out


def sparkBronKerboschl(graph, x):
    graph = graph.value
    v = x.vId
    # R = Sorted_List_Set([])
    All_elems = SortedListSet([i for i in range(graph.get_vertex_num())])
    X = All_elems.elems_to(v)
    P = All_elems.elems_from(v)

    nb = x.neighbours
    return recBronKerboschl(SortedListSet([v]),P.intersect(nb),X.intersect(nb),graph)


def bronKerboschl(sc,graph):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    return graph \
        .get_edges() \
        .flatMap(lambda x: sparkBronKerboschl(non_spark_graph,x))\
        .collect()