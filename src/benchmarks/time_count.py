import time
import numpy as np
from src.algorithms.BronKerboschl import bron_kerboschl
from src.algorithms.CliqueCount import clique_count, rec_clique_count
from src.algorithms.KCliqueListing import rec_clique_listing, clique_listing
from src.algorithms.KCliqueStar import k_clique_star
from src.algorithms.triangleCount import triangle_count, triangle_count_per_vertex
from src.graphs.rddGraphSet import CustomRow, RDDGraphSet
from src.sets.set import SortedListSet, HashSet
from src.graph_creation import *
from src.util.printing import *


RETRIES = 2


if __name__ == '__main__':
    tested_graph_file_path = "../graphDatasets/test_graph_edges.csv"
    spark = SparkSession.builder \
        .master("local") \
        .getOrCreate()
    sc = spark.sparkContext

    graph_types = {"rddGraphSet": create_undirected_graph}
    sets = {"SortedListSet" : SortedListSet,"HashSet" : HashSet}
    tested_algorithms = {"BronKerboschl" : (bron_kerboschl,),
                         "Clique Count (3)" : (clique_count,3),
                         "Clique Listing (3)": (clique_listing,3),
                         "Triangle Count" : (triangle_count,)}

    graph_combs ={}
    for graph_type, graph_type_cons in graph_types.items():
        l1_dict = {}
        for set_name,set_cons in sets.items():
            l1_dict[set_name] = create_undirected_graph(spark,tested_graph_file_path,set_cons)
        graph_combs[graph_type] = l1_dict


    out_lines = []
    for algorithm_name, alg_tuple in tested_algorithms.items():
        out_lines.append(algorithm_name)
        longest_graph_type_name = max([len(key) for key in graph_combs.keys()])

        for graph_type in graph_combs:
            longest_graph_set_name = max([len(key) for key in graph_combs[graph_type].keys()])
            out_lines.append(''.join([' ']*20) + print_in_n_chars(graph_type,longest_graph_type_name+5))
            for graph_set,graph_obj in graph_combs[graph_type].items():
                single_call = None
                if len(alg_tuple) ==1:
                    single_call = lambda : alg_tuple[0](sc,graph_obj)
                elif len(alg_tuple) ==2:
                    single_call = lambda: alg_tuple[0](sc, graph_obj,alg_tuple[1])
                times = []
                t0 = time.time()
                for i in range(RETRIES):
                    single_call()
                    times.append(time.time())
                times_per_call = [ times[i] - times[i-1] if i>0 else times[i] -t0 for i in range(len(times))]
                total_time = np.sum(times_per_call)/len(times_per_call)
                time_stddev = np.std(times_per_call) if len(times_per_call) > 1 else 0
                out_lines.append(''.join([' '] * (20 + longest_graph_type_name))+
                                 print_in_n_chars(graph_set, longest_graph_set_name + 5)+
                                 f'time: {total_time:.5f}s stddev : {time_stddev:.5f} ')
    print('')
    print('')
    for line in out_lines:
        print(line)

    input('enter to crash')
    spark.stop()

