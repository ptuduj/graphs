from functools import reduce
from src.sets.set import HashSet
from scipy import special

def _test_distinc_list(list):
    out =[]
    is_in = set()
    for el in list:
        if not el in is_in:
            is_in.add(el)
            out.append(el)
    return out

def triangle_count_per_edge(graph, x):
    u = x[0]
    v = x[1]
    nb_u = graph.out_neighbours(u)
    nb_v = graph.out_neighbours(v)
    return len(nb_u.intersect(nb_v))


def triangle_count_edge_parallel(sc, graph):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    return graph \
        .get_edges() \
        .map(lambda edge: triangle_count_per_edge(non_spark_graph.value, edge))\
        .sum()

def triangle_count_per_edge_v_graph(graph, x): #

    u = x[0]
    v = x[1]
    nb_u = graph.out_neighbours(u)
    nb_v = graph.out_neighbours(v)
    return len(nb_u.intersect(nb_v))

def triangle_count_v_nodes(graph,vn): ## x[1] is a vnode
    assert graph.is_virtual(vn)
    k = len(graph.out_neighbours(vn))
    clique_triangles = special.comb(k, 3, exact=True)
    nb_list = list(graph.out_neighbours(vn))
    out = 0
    for i in range(len(nb_list)-1):
        for j in range(i+1,len(nb_list)):
           out += triangle_count_per_edge_v_graph(graph,(i,j))

    return clique_triangles + out #analitical count of triangles in clique


# def test_spark_less_triangle_count_edge_parallel_v_graph(sc,graph):
#     slg = graph.get_spark_less_copy()
#     edges = graph.get_edges().collect()
#     non_virtual_sum = list(filter(lambda x: (not slg.is_virtual(x[1]) and not slg.is_virtual(x[0])), edges))
#     print(non_virtual_sum)
#     non_virtual_sum = list(map(lambda x: triangle_count_per_edge_v_graph(slg, x), non_virtual_sum))
#     print(non_virtual_sum)
#     virtual_sum = list(filter(lambda x: slg.is_virtual(x[0]), edges))
#     print(virtual_sum)
#     virtual_sum = list(map(lambda x: x[0], virtual_sum))
#     print(virtual_sum)
#     virtual_sum = _test_distinc_list(virtual_sum)
#     print(virtual_sum)
#     virtual_sum = list(map(lambda x: triangle_count_v_nodes(slg, x), virtual_sum))
#     print(virtual_sum)
#
#     return sum(virtual_sum) + sum(non_virtual_sum)


def triangle_count_edge_parallel_v_graph(sc,graph):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    non_virtual_sum = graph \
        .get_edges() \
        .filter(lambda x:not non_spark_graph.value.is_virtual(x[0])) \
        .map(lambda edge: triangle_count_per_edge_v_graph(non_spark_graph.value, edge))\
        .sum()

    # print('non_virtual_sum')
    # print(non_virtual_sum)
    virtual_sum = graph \
        .get_edges() \
        .filter(lambda x: non_spark_graph.value.is_virtual(x[0])) \
        .map(lambda x: x[0]) \
        .distinct() \
        .map(lambda x: triangle_count_v_nodes(non_spark_graph.value,x)) \
        .sum()
    print('virtual_sum')
    print(virtual_sum)
    return non_virtual_sum+virtual_sum