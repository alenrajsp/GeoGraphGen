import multiprocessing
from collections import defaultdict
from typing import Dict

from geo_classes.DuplicatePathRemover import DuplicatePathRemover
from setup import GeoConnector
import time
from pymongo.database import Collection
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from setup.Constants import MULTI


class PathOptimizationProposal:
    def __init__(self, node, a_nodes, b_nodes, merge=False):
        self.node = node
        self.a_nodes = a_nodes
        self.b_nodes = b_nodes
        self.a_relationships = []
        self.b_relationships = []
        self.merge = merge
        self.new_relationship = None

    def to_merge_proposal(self):
        if len(self.a_nodes) != 2 or len(self.b_nodes) != 2:
            return False
        for node in self.a_nodes:
            if node not in self.b_nodes:
                return False
        for node in self.b_nodes:
            if node not in self.a_nodes:
                return False
        for node in self.a_nodes:
            if self.a_nodes.count(node) > 1 or self.b_nodes.count(node) > 1:
                return False
        self.new_relationship = set(self.a_nodes + self.b_nodes)

        return True


class ConnectionMerger:
    def __init__(self, geo_connector: GeoConnector):
        self.geo_connector = geo_connector

    def worker(self, batch):
        client: MongoClient = self.geo_connector.mongo_db()
        session = client.start_session()
        for merger in batch:
            self.merge_relationship(merger, client, session)
        session.end_session()
        client.close()

    async def merge(self):
        c = True
        duplicate_remover = DuplicatePathRemover(self.geo_connector)

        index = 0

        with open("merge_log.txt", "a") as file:
            file.write(f"Index, Total relationships, Total nodes\n")

        while c is True:
            client = self.geo_connector.mongo_db()

            total_relationships_before = client["geo_data"].paths.count_documents({})
            total_nodes_before = client["geo_data"].intersections.count_documents({})

            with open("merge_log.txt", "a") as file:
                file.write(f"{index}, {total_relationships_before}, {total_nodes_before}\n")

            print("#" * 50)
            print(f"Merging intersections stage {index}")
            start = time.time()

            duplicate_remover.identify_duplicates()

            mergers = self.identify_mergers()

            if MULTI is False:
                client: MongoClient = self.geo_connector.mongo_db()
                session = client.start_session()
                for merger in mergers:
                    self.merge_relationship(
                        merger,
                        client,
                        session,
                    )
            else:
                pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)
                batch_size = 2000
                for i in range(0, len(mergers), batch_size):
                    pool.apply_async(self.worker, args=(mergers[i : i + batch_size],))
                pool.close()
                pool.join()

            end = time.time()
            total_relationships_after = client["geo_data"].paths.count_documents({})
            print(
                f"{index}, {end - start}, {len(mergers)}, {total_relationships_before}, {total_relationships_after}\n"
            )
            print("#" * 50)
            if len(mergers) == 0:
                c = False
            index += 1
        total_relationships = client["geo_data"].paths.count_documents({})
        total_nodes = client["geo_data"].intersections.count_documents({})
        with open("merge_log.txt", "a") as file:
            file.write(f"{index}, {total_relationships}, {total_nodes}\n")

    def merge_nodes(self, nodes_a, nodes_b):
        # Initialize an empty dictionary to keep track of seen node IDs
        seen_ids = {}
        unique_nodes = []

        # Iterate over the concatenated nodes from both documents
        for node in nodes_a + nodes_b:
            node_id = node["id"]
            if node_id not in seen_ids:
                seen_ids[node_id] = node
                unique_nodes.append(node)
        return unique_nodes

    def remove_nodes(self, skip=None, limit=None):
        geo_connector: MongoClient = self.geo_connector.mongo_db()
        paths: Collection = geo_connector["geo_data"].paths
        intersections: Collection = geo_connector["geo_data"].intersections
        if skip is not None and limit is not None:
            batch = intersections.find().skip(skip).limit(limit)
        else:
            batch = intersections.find()

        for i in batch:
            node_id = i["_id"]
            count_start = paths.count_documents({"start_node": node_id})
            count_end = paths.count_documents({"end_node": node_id})
            if count_start == 0 and count_end == 0:
                intersections.delete_one({"_id": node_id})
        geo_connector.close()

    def remove_unused_nodes(self):
        geo_connector: MongoClient = self.geo_connector.mongo_db()
        intersections: Collection = geo_connector["geo_data"].intersections
        paths = geo_connector["geo_data"].paths

        query = {"$expr": {"$eq": ["$start_node", "$end_node"]}}
        result = paths.delete_many(query)

        if MULTI is False:
            intersections: Collection = geo_connector["geo_data"].intersections
            self.remove_nodes()
        else:
            pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)
            batch_size = 10000
            for i in range(0, intersections.count_documents({}), batch_size):
                skip = i
                limit = batch_size
                pool.apply_async(
                    self.remove_nodes,
                    args=(
                        skip,
                        limit,
                    ),
                )
            pool.close()
            pool.join()

    def merge_path_doc(self, doc1, doc2):
        merged_doc = {
            "start_node": doc1["start_node"],
            "end_node": doc2["end_node"],
            "distance": doc1["distance"] + doc2["distance"],
            "ascent": doc1["ascent"] + doc2["ascent"],
            "descent": doc1["descent"] + doc2["descent"],
            "path_type": list(set(doc1["path_type"] + doc2["path_type"])),
            "surface": list(set(doc1["surface"] + doc2["surface"])),
            "curviness": (doc1["curviness"] * doc1["distance"] + doc2["curviness"] * doc2["distance"])
            / (doc1["distance"] + doc2["distance"]),
            "traffic_lights": doc1["traffic_lights"] + doc2["traffic_lights"],
            "total_angle": doc1["total_angle"] + doc2["total_angle"],
            "hill_flat": doc1["hill_flat"] + doc2["hill_flat"],
            "hill_gentle": doc1["hill_gentle"] + doc2["hill_gentle"],
            "hill_moderate": doc1["hill_moderate"] + doc2["hill_moderate"],
            "hill_challenging": doc1["hill_challenging"] + doc2["hill_challenging"],
            "hill_steep": doc1["hill_steep"] + doc2["hill_steep"],
            "hill_extremely_steep": doc1["hill_extremely_steep"] + doc2["hill_extremely_steep"],
            "forward": doc1["forward"] and doc2["forward"],
            "backward": doc1["backward"] and doc2["backward"],
            "bicycle_access": doc1["bicycle_access"] and doc2["bicycle_access"],
            "foot_access": doc1["foot_access"] and doc2["foot_access"],
            "car_access": doc1["car_access"] and doc2["car_access"],
            "valid": doc1["valid"] and doc2["valid"],
            "nodes": self.merge_nodes(doc1["nodes"], doc2["nodes"]),
        }
        return merged_doc

    def merge_relationship(self, merger: PathOptimizationProposal, client: MongoClient, session):
        paths: Collection = client["geo_data"].paths
        intersections: Collection = client["geo_data"].intersections

        try:
            node_id_a = list(merger.new_relationship)[0]
            node_id_b = merger.node
            node_id_c = list(merger.new_relationship)[1]
        except:
            print("Error in merging the paths")
            return

        if merger.a_nodes[0] == merger.b_nodes[0] or merger.a_nodes[1] == merger.b_nodes[1]:
            path_forward_part_1 = paths.find_one({"_id": merger.a_relationships[0]})
            path_forward_part_2 = paths.find_one({"_id": merger.b_relationships[1]})
            path_backward_part_1 = paths.find_one({"_id": merger.a_relationships[1]})
            path_backward_part_2 = paths.find_one({"_id": merger.b_relationships[0]})
        else:
            path_forward_part_1 = paths.find_one({"_id": merger.a_relationships[0]})
            path_forward_part_2 = paths.find_one({"_id": merger.b_relationships[0]})
            path_backward_part_1 = paths.find_one({"_id": merger.a_relationships[1]})
            path_backward_part_2 = paths.find_one({"_id": merger.b_relationships[1]})

        count_forward_part_1 = paths.count_documents({"start_node": node_id_a, "end_node": node_id_b})
        count_forward_part_2 = paths.count_documents({"start_node": node_id_b, "end_node": node_id_c})
        count_backward_part_1 = paths.count_documents({"start_node": node_id_c, "end_node": node_id_b})
        count_backward_part_2 = paths.count_documents({"start_node": node_id_b, "end_node": node_id_a})

        if (
            count_forward_part_1 != 1
            or count_forward_part_2 != 1
            or count_backward_part_1 != 1
            or count_backward_part_2 != 1
        ):
            print(
                f"Error in merging the paths, counts: {count_forward_part_1}, {count_forward_part_2}, {count_backward_part_1}, {count_backward_part_2}"
            )
            raise Exception("Error in merging the paths")

        if (
            path_forward_part_1 is None
            or path_forward_part_2 is None
            or path_backward_part_1 is None
            or path_backward_part_2 is None
        ):
            raise Exception("One of the paths is missing")

        path_forward = self.merge_path_doc(path_forward_part_1, path_forward_part_2)
        path_backward = self.merge_path_doc(path_backward_part_1, path_backward_part_2)

        try:
            # Start a transaction
            with session.start_transaction():
                # Update the paths
                paths.delete_one({"_id": path_forward_part_1["_id"]})
                paths.delete_one({"_id": path_forward_part_2["_id"]})
                paths.delete_one({"_id": path_backward_part_1["_id"]})
                paths.delete_one({"_id": path_backward_part_2["_id"]})

                paths.insert_one(path_forward)
                paths.insert_one(path_backward)

                intersections.delete_one({"_id": merger.node})

                session.commit_transaction()
        except ConnectionFailure:
            print("Error in merging the paths")
        a = 100

    def identify_mergers(self):
        mongo: MongoClient = self.geo_connector.mongo_db()
        db = mongo.geo_data
        collection = db.paths

        node_merge_proposals: Dict[int, PathOptimizationProposal] = defaultdict(
            lambda: PathOptimizationProposal(None, [], [])
        )

        paths = collection.find({}, {"_id": 1, "start_node": 1, "end_node": 1})
        i = 0
        total_paths = collection.count_documents({})  # Optional: use to limit print updates

        for path in paths:
            i += 1

            # Start node handling
            pop_start = node_merge_proposals[path["start_node"]]
            if pop_start.node is None:
                pop_start.node = path["start_node"]
            pop_start.b_nodes.append(path["end_node"])
            pop_start.b_relationships.append(path["_id"])

            # End node handling
            pop_end = node_merge_proposals[path["end_node"]]
            if pop_end.node is None:
                pop_end.node = path["end_node"]
            pop_end.a_nodes.append(path["start_node"])
            pop_end.a_relationships.append(path["_id"])

            # Print progress (optional, adjust frequency)
            if i % (total_paths // 100) == 0:  # Print at 1% intervals
                print(f"Processed {i}/{total_paths} paths", end="\r")

        print(f"\nFinished processing {i} paths.")

        i = 0
        locked_node_ids = {}
        to_merge = 0
        mergers = []
        for node_id, pop in node_merge_proposals.items():
            if node_id in locked_node_ids:
                continue
            pop.merge = pop.to_merge_proposal()
            i += 1
            if pop.merge:
                to_merge += 1
                mergers.append(pop)
                locked_node_ids[node_id] = True
                for node in pop.new_relationship:
                    locked_node_ids[node] = True
        print(f"To reduce: {to_merge*2} Total: {total_paths} ")

        return mergers
