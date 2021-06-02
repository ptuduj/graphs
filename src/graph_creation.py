import csv
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.types as T
from pyspark.sql.functions import col, concat

from src.graphs.rddGraphSet import CustomRow, RDDGraphSet, EdgeListGraphSet


def map_row(row):
    if row["id_1"] == None:
        return (row["id_2"], row["neighbours1"], row["id_2"], row["neighbours2"])
    if row["id_2"] == None:
        return (row["id_1"], row["neighbours1"], row["id_1"], row["neighbours2"])
    return row


def create_undirected_graph_from_csv(spark,file_name,chosen_set_const):
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
    rdd = df_result.rdd.map(lambda row: CustomRow(row["id"], chosen_set_const(row["neighbours"], from_sorted=False)))
    return RDDGraphSet(rdd)


def create_edgeListGraph(spark, filename, chosen_set_const):
    reader = csv.reader(open(filename, 'r'))
    headers = next(reader, None)
    edges = []
    for id1, id2 in reader:
        id1 = int(id1)
        id2 = int(id2)
        if id1 < id2:
            edges.append((id1, id2))
        else:
            edges.append((id2, id1))

    sc = spark.sparkContext
    edge_rdd = sc.parallelize(edges)

    df = spark.createDataFrame(edges, headers)
    rdd = df.groupBy("id_1").agg(F.collect_list("id_2").alias("neighbours")).orderBy(df["id_1"].asc()).rdd
    custom_rows_rdd = rdd.map(lambda row: CustomRow(row["id_1"], chosen_set_const(row["neighbours"], from_sorted=False)))
    return EdgeListGraphSet(edge_rdd, custom_rows_rdd)

def create_rddGraphSet(spark, filename, chosen_set_const):
    reader = csv.reader(open(filename, 'r'))
    headers = next(reader, None)
    edges = []
    for id1, id2 in reader:
        id1 = int(id1)
        id2 = int(id2)
        if id1 < id2:
            edges.append((id1, id2))
        else:
            edges.append((id2, id1))

    df = spark.createDataFrame(edges, headers)
    rdd = df.groupBy("id_1").agg(F.collect_list("id_2").alias("neighbours")).orderBy(df["id_1"].asc()).rdd
    custom_rows_rdd = rdd.map(lambda row: CustomRow(row["id_1"], chosen_set_const(row["neighbours"], from_sorted=False)))
    return RDDGraphSet(custom_rows_rdd)

