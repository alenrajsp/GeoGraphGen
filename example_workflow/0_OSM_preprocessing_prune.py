import osmium
import os


class NodesIdentifierHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.count_nodes = 0
        self.count_ways = 0
        self.highway_nodes = set()

    def node(self, n):
        self.count_nodes += 1

    def way(self, w):
        if "highway" in w.tags:
            self.highway_nodes.update([n.ref for n in w.nodes])


class HighwayNodesWaysHandler(osmium.SimpleHandler):
    def __init__(self, highway_nodes_set, writer_pbf, writer_bz2):
        super().__init__()
        self.highway_nodes_set = highway_nodes_set
        self.writer_pbf = writer_pbf
        self.writer_bz2 = writer_bz2
        self.count_nodes = 0
        self.count_ways = 0

    def way(self, w):
        if "highway" in w.tags:
            self.writer_pbf.add_way(w)
            self.writer_bz2.add_way(w)
            self.count_ways += 1

    def node(self, n):
        if n.id in self.highway_nodes_set:
            self.writer_pbf.add_node(n)
            self.writer_bz2.add_node(n)
            self.count_nodes += 1


def main():
    current_directory = os.getcwd()
    input_file = os.path.join(current_directory, "data", "input.osm.pbf")
    output_pbf = os.path.join(current_directory, "data", "output.osm.pbf")
    output_bz2 = os.path.join(current_directory, "data", "output.osm.bz2")

    print("Collecting highway nodes...")
    nc = NodesIdentifierHandler()
    nc.apply_file(input_file)

    print("#" * 50)
    print(f"Original file has {nc.count_nodes} nodes and {nc.count_ways} ways.")
    print("#" * 50)
    print("\n")

    print("Identified highway nodes:", len(nc.highway_nodes))

    file_writer_pbf = osmium.SimpleWriter(output_pbf)
    file_writer_bz = osmium.SimpleWriter(output_bz2)

    print("Writing highway nodes and ways to output files...")
    h = HighwayNodesWaysHandler(nc.highway_nodes, file_writer_pbf, file_writer_bz)
    h.apply_file(input_file)

    print("#" * 50)
    print(f"Output file has {h.count_nodes} nodes and {h.count_ways} ways.")
    print("#" * 50)
    print("\n")

    file_writer_pbf.close()
    file_writer_bz.close()

    print("Done. Files are saved in the data folder.")


if __name__ == "__main__":
    main()
