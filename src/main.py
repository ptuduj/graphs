import csv
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.types as T
from pyspark.sql.functions import col, concat

from src.algorithms.BronKerboschl import bronKerboschl
from src.algorithms.CliqueCount import clique_count, rec_clique_count
from src.algorithms.triangleCount import triangle_count, triangle_count_per_vertex
from src.graphs.rddGraphSet import CustomRow, RDDGraphSet
from src.sets.sortedListSet import SortedListSet

def map_row(row):
    if row["id_1"] == None:
        return (row["id_2"], row["neighbours1"], row["id_2"], row["neighbours2"])
    if row["id_2"] == None:
        return (row["id_1"], row["neighbours1"], row["id_1"], row["neighbours2"])
    return row

def create_undirected_graph_from_csv(file_name):
    df = spark.read.format("csv").option("header", "true").load(file_name)
    df = df.withColumn("id_1", df["id_1"].cast(IntegerType()))
    df = df.withColumn("id_2", df["id_2"].cast(IntegerType()))

    df1 = df.groupBy("id_1").agg(F.collect_list("id_2").alias("neighbours1"))
    df2 = df.groupBy("id_2").agg(F.collect_list("id_1").alias("neighbours2"))

    df = df1.join(df2, df1["id_1"] == df2["id_2"], "fullouter")
    df = df.withColumn(
        "neighbours1", F.coalesce(
            F.col("neighbours1"), F.from_json(F.lit("[]"), T.ArrayType(T.IntegerType()))
        )
    )
    df = df.withColumn(
        "neighbours2",
        F.coalesce(F.col("neighbours2"), F.from_json(F.lit("[]"), T.ArrayType(T.IntegerType())))
    )

    df = df.rdd.map(lambda row: map_row(row)).toDF()
    df_result = df.select(col("_1").alias("id"), concat(col("_2"), col("_4")).alias("neighbours"))
    rdd = df_result.rdd.map(lambda row: CustomRow(row["id"], SortedListSet(row["neighbours"], from_sorted=False)))
    return RDDGraphSet(rdd)

def create_undirected_graph(filename):
    reader = csv.reader(open(filename, 'r'))
    headers = next(reader, None)
    edges = []
    for id1, id2 in reader:
        edges.append((id1, id2))
        edges.append((id2, id1))

    df = spark.createDataFrame(edges, headers)
    df = df.withColumn("id_1", df["id_1"].cast(IntegerType()))
    df = df.withColumn("id_2", df["id_2"].cast(IntegerType()))
    rdd = df.groupBy("id_1").agg(F.collect_list("id_2").alias("neighbours")).orderBy(df["id_1"].asc()).rdd
    rdd = rdd.map(lambda row: CustomRow(row["id_1"], SortedListSet(row["neighbours"], from_sorted=False)))
    rdd.toDF().show()
    return RDDGraphSet(rdd)


if __name__ == '__main__':

    spark = SparkSession.builder \
        .master("local") \
        .getOrCreate()
    sc = spark.sparkContext
    graph = create_undirected_graph("graphDatasets/test_graph_edges.csv")
    #row1 = graph.get_edges().filter(lambda row: row.vId == 1).first()


    # print("Start count k-cliques")
    # print(clique_count(sc, graph, 3))
    #print(rec_clique_count(graph.get_spark_less_copy(), 4, row1.vId, row1.neighbours))

    print('BronKerboschl')
    l = bronKerboschl(sc, graph)
    for elem in l:
        print(elem)

    # print('triangle count')
    # print(triangle_count(sc, graph))
   #  print(triangle_count_per_vertex(graph.get_spark_less_copy(), row1))


    input('enter to crash')
    spark.stop()

