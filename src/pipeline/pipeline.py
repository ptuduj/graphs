import time

from pyspark.sql import SparkSession

from src.benchmarks.benchmarks import Benchmarks
from src.graph_creation import create_graph


class Pipeline:
    def __init__(self, filename, chosen_set_repr, algorithms_dict, benchmarks_list,
                 chosen_graph_repr, preprocessing_list):

        self.filename = filename
        self.chosen_set_repr = chosen_set_repr
        self.algorithms_dict = algorithms_dict
        self.benchmarks_list = benchmarks_list
        self.chosen_graph_repr = chosen_graph_repr
        self.preprocessing_list = preprocessing_list

        self.graph = None
        self.spark = self._init_spark_context()


    def _init_spark_context(self):
        return SparkSession.builder \
            .master("local") \
            .getOrCreate()


    def create_graph(self):
        self.graph = create_graph(self.spark, self.filename, self.chosen_set_repr,
                                  self.chosen_graph_repr, self.preprocessing_list)
        return self.graph


    def run_all(self):
        self.create_graph()
        res = []
        for algorithm_fun, params in self.algorithms_dict.items():
            res.append(self.calc_benchmarks(algorithm_fun, params, self.benchmarks_list))
        return res

    def calc_benchmarks(self, algorithm_fun, params, benchmarks):
        sc = self.spark.sparkContext

        t_start = time.time()
        if len(params) == 0:
            res = algorithm_fun(sc, self.graph)
        elif len(params) == 1:
            res = algorithm_fun(sc, self.graph, params[0])

        t = time.time() - t_start
        benchmarks_dict = {}

        for b in benchmarks:
            if b == Benchmarks.TimeCount:
                benchmarks_dict["time count"] = t
            elif b == Benchmarks.ProcessedEdgesPerSec:
                benchmarks_dict["processed edges per sec"] = self.graph.edges_count() / t
            elif b == Benchmarks.ProcessedVerticesPerSec:
                benchmarks_dict["processed vertices per sec"] = self.graph.vertices_count() / t

        #benchmarks_dict["res"] = res
        return benchmarks_dict




