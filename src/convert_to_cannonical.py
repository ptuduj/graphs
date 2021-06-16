if __name__ == '__main__':

    edges_file = "graphDatasets/soc-pokec-relationships.csv"
    new_edges_file = "soc-pokec-relationships_new.csv"
    edges_set = set()
    i = 0
    with open(edges_file, 'r') as file:
        for line in file:
            i += 1
            out = line.split('\n')
            vertices = out[0].split('\t')
            if vertices[0] == "id_1":
                continue

            u = int(vertices[0])
            v = int(vertices[1])
            if u == v:
                continue
            if u < v:
                u, v = v, u
            if not (u, v) in edges_set:
                edges_set.add((u, v))

    print(i, len(edges_set))

    with open(new_edges_file, 'w') as file:
        file.write("id_1,id_2" + "\n")
        for edge_tup in edges_set:
            file.write(str(edge_tup[0]) + ',' + str(edge_tup[1]) + '\n')

