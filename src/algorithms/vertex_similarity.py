import math

def vertex_similarity_jaccard(u, v, graph):
    out_neighb_u, out_neighb_v = graph.out_neighbours(u), graph.out_neighbours(v)
    if len(out_neighb_u) == 0 and len(out_neighb_v) == 0:
        return 1.0

    count = out_neighb_u.intersect_count(out_neighb_v)
    return count / (len(out_neighb_u) + len(out_neighb_v) + count)


def vertex_similarity_overlap(u, v, graph):
    out_neighb_u, out_neighb_v = graph.out_neighbours(u), graph.out_neighbours(v)
    overlap = len(out_neighb_u.intersect(out_neighb_v))
    return overlap / min(len(out_neighb_u), len(out_neighb_v))


def vertex_similarity_common_neighbors(u, v, graph):
    out_neighb_u, out_neighb_v = graph.out_neighbours(u), graph.out_neighbours(v)
    return len(out_neighb_u.intersect(out_neighb_v))


def vertex_similarity_adamic_adar(u, v, graph):
    out_neighb_u, out_neighb_v = graph.out_neighbours(u), graph.out_neighbours(v)
    intersection = out_neighb_u.intersect(out_neighb_v)
    return sum(intersection.map(lambda vertex:  1.0 / math.log(graph.out_degree(vertex))))


def vertex_similarity_total_neighbors(u, v, graph):
    out_neighb_u, out_neighb_v = graph.out_neighbours(u), graph.out_neighbours(v)
    return len(out_neighb_u.union(out_neighb_v))

