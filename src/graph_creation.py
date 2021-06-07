import csv
from pyspark.sql import functions as F
from src.graphs.rddGraphSet import CustomRow, RDDGraphSet, EdgeListGraphSet, GraphType, GraphRepresentation


def parse_csv(graph_type, filename):
    reader = csv.reader(open(filename, 'r'))
    headers = next(reader, None)
    edges = set()

    for id1, id2 in reader:
        id1, id2 = int(id1), int(id2)
        if id1 == id2:
            continue
        if graph_type == GraphType.DIRECTED:
            if id1 < id2:
                edges.add((id1, id2))
            else:

                edges.add((id2, id1))

        elif graph_type == GraphType.UNDIRECTED:
            edges.add((id1, id2))
            edges.add((id2, id1))
    return headers, list(edges)


def create_graph_repr(chosen_graph_repr, edge_rdd, custom_rows_rdd):
    if chosen_graph_repr == GraphRepresentation.RDDGraphSet:
        return RDDGraphSet(custom_rows_rdd)

    elif chosen_graph_repr == GraphRepresentation.EdgeListGraphSet:
        return EdgeListGraphSet(edge_rdd, custom_rows_rdd)


def create_graph(spark, filename, chosen_set_repr, chosen_graph_repr, graph_type):
    headers, edges = parse_csv(graph_type, filename)

    sc = spark.sparkContext
    edge_rdd = sc.parallelize(edges)

    df = spark.createDataFrame(edges, headers)
    rdd = df.groupBy("id_1").agg(F.collect_list("id_2").alias("neighbours")).orderBy(df["id_1"].asc()).rdd
    custom_rows_rdd = rdd.map(lambda row: CustomRow(row["id_1"], chosen_set_repr(row["neighbours"], from_sorted=False)))
    return create_graph_repr(chosen_graph_repr, edge_rdd, custom_rows_rdd)


