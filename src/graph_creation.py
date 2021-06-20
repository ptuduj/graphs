from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.graphs.rddGraphSet import CustomRow, RDDGraphSet, EdgeRDDGraph, GraphRepresentation


def create_graph_repr(chosen_graph_repr, edge_rdd, custom_rows_rdd):
    if chosen_graph_repr == GraphRepresentation.RDDGraphSet:
        return RDDGraphSet(custom_rows_rdd)

    elif chosen_graph_repr == GraphRepresentation.EdgeRDDGraph:
        return EdgeRDDGraph(edge_rdd, custom_rows_rdd)


def change_edge_orientation(r):
    if r[0] < r[1]:
        return (r[0], r[1])
    return (r[1], r[0])



def remove_self_edges(edges_rdd):
    return edges_rdd.filter(lambda x: x[0] != x[1])


def make_edges_oriented(edges_rdd):
    return edges_rdd.map(lambda x: change_edge_orientation(x))


def remove_duplicate_edges(edges_rdd):
    return edges_rdd.distinct()


def make_graph_undirected(edges_rdd):
    return edges_rdd.flatMap(lambda x: [x, (x[1], x[0])])

def find_longest_path(graph, vertex):
    vertices_to_remove = [vertex]
    neighb = list(graph.out_neighbours(vertex))[0]
    next = neighb
    while graph.out_degree(next) == 2:
        vertices_to_remove.append(next)
        next = list(graph.out_neighbours(next))[0]
    return vertices_to_remove


def remove_paths(sc, graph):
    non_spark_graph = sc.broadcast(graph.get_spark_less_copy())
    lonely_vertices = graph._rdd_custom_rows\
        .filter(lambda row: len(row.neighbours) == 1)\
        .map(lambda row: row.vId)\
        .flatMap(lambda v: find_longest_path(non_spark_graph.value, v))\
        .collect()

    new_custom_rows = graph._rdd_custom_rows.filter(lambda row: row.vId not in lonely_vertices)
    print(len(lonely_vertices))
    print(graph._rdd_custom_rows.count())
    print(new_custom_rows.count())
    #return create_graph_repr(GraphRepresentation.RDDGraphSet, None, new_custom_rows)




def create_graph(spark,
                 filename,
                 chosen_set_representation,
                 chosen_graph_representation,
                 preprocessing_list):

    edges_rdd = spark.read.options(header='True', delimiter="\t").csv(filename).rdd

    for preprocessing_fun in preprocessing_list:
        edges_rdd = preprocessing_fun(edges_rdd)

    edges_df = spark.createDataFrame(edges_rdd).toDF("id_1", "id_2")
    edges_df = edges_df.withColumn("id_1", edges_df["id_1"].cast(IntegerType()))
    edges_df = edges_df.withColumn("id_2", edges_df["id_2"].cast(IntegerType()))
    edges_rdd = edges_df.rdd
    edges_df = edges_df.groupBy("id_1").agg(F.collect_list("id_2").alias("neighbours"))

    custom_rows_rdd = edges_df.rdd.map(lambda row: CustomRow(row["id_1"], chosen_set_representation(row["neighbours"], from_sorted = False)))
    return create_graph_repr(chosen_graph_representation, edges_rdd, custom_rows_rdd)

def create_both_representations(spark,
                 filename,
                 chosen_set_representation,
                 preprocessing_list):

    edges_rdd = spark.read.options(header='True').csv(filename).rdd

    for preprocessing_fun in preprocessing_list:
        edges_rdd = preprocessing_fun(edges_rdd)

    edges_df = spark.createDataFrame(edges_rdd).toDF("id_1", "id_2")
    edges_df = edges_df.withColumn("id_1", edges_df["id_1"].cast(IntegerType()))
    edges_df = edges_df.withColumn("id_2", edges_df["id_2"].cast(IntegerType()))
    edges_rdd = edges_df.rdd
    edges_df = edges_df.groupBy("id_1").agg(F.collect_list("id_2").alias("neighbours"))

    custom_rows_rdd = edges_df.rdd.map(
        lambda row: CustomRow(row["id_1"], chosen_set_representation(row["neighbours"], from_sorted=False)))
    return create_graph_repr(GraphRepresentation.RDDGraphSet, edges_rdd, custom_rows_rdd),\
            create_graph_repr(GraphRepresentation.EdgeRDDGraph,edges_rdd,custom_rows_rdd)

