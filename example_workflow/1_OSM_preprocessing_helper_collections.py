import os

import osmium
import pymongo
from collections import defaultdict
from setup.Constants import MONGO_DB_ADDRESS, MONGO_DB_PORT, MONGO_DB_USERNAME, MONGO_DB_PASSWORD


class OSMHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.highways = []
        self.nodes = defaultdict(lambda: {"ways": []})
        self.node_count = 0
        self.way_count = 0

    def node(self, n: osmium.Node):
        self.nodes[n.id].update({"_id": n.id, "lat": n.location.lat, "lon": n.location.lon, "tags": dict(n.tags)})
        self.node_count += 1
        if self.node_count % 100000 == 0:
            print(f"Processed {self.node_count} nodes...")

    def way(self, w: osmium.Way):
        relevant_tags = {
            "bicycle",
            "foot",
            "highway",
            "oneway",
            "surface",
            "sidewalk",
            "access",
            "motorcar",
            "motor_vehicle",
            "crossing",
            "bridge",
            "access",
            "traffic_signals",
            "cycleway",
        }
        tags = {k: v for k, v in dict(w.tags).items() if k in relevant_tags}

        if "highway" in w.tags:
            highway_info = {"_id": w.id, "tags": tags, "nodes": [n.ref for n in w.nodes]}
            self.highways.append(highway_info)
            for node_ref in w.nodes:
                self.nodes[node_ref.ref]["ways"].append(w.id)
        self.way_count += 1
        if self.way_count % 10000 == 0:
            print(f"Processed {self.way_count} ways...")


def resolve_nodes(handler):
    for highway in handler.highways:
        resolved_nodes = [handler.nodes[node_ref] for node_ref in highway["nodes"] if node_ref in handler.nodes]
        highway["nodes"] = resolved_nodes


def save_to_mongo(collection, data):
    if data:
        collection.insert_many(data)


if __name__ == "__main__":
    current_directory = os.getcwd()
    input_file = os.path.join(current_directory, "data", "output.osm.pbf")

    print(f"Processing file: {input_file}")
    h = OSMHandler()
    h.apply_file(input_file)

    # Resolve node references in highways
    print("Resolving node references in highways...")
    resolve_nodes(h)

    mongo_uri = (
        f"mongodb://{MONGO_DB_USERNAME}:{MONGO_DB_PASSWORD}@{MONGO_DB_ADDRESS}:{MONGO_DB_PORT}/?authSource=admin"
    )

    print(mongo_uri)
    client = pymongo.MongoClient(mongo_uri)
    db = client["geo_data"]
    highways_collection = db["highways_helper"]
    nodes_collection = db["nodes_helper"]

    print("Saving data to MongoDB...")
    # Save the highways data to MongoDB
    save_to_mongo(highways_collection, h.highways)

    # Save the nodes data to MongoDB
    node_data = list(h.nodes.values())
    save_to_mongo(nodes_collection, node_data)

    print("#" * 50)
    print("Total nodes processed:", h.node_count)
    print("Total ways processed:", h.way_count)

    print("Data saved successfully.")
