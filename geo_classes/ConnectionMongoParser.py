"""3. Find intersections from neo4j and identify pathways between them"""
import multiprocessing
import time
import overpy
from pymongo.collection import Collection
from pymongo import ASCENDING
from graph.Pathway import Pathway

from setup.GeoConnector import GeoConnector
from setup.MapBoundaries import MapBoundaries
from setup.Constants import MULTI, OVERPASS_API_URL
import pymongo
from datetime import datetime


class ConnectionMongoParser:
    def __init__(self, geo_connector: GeoConnector):
        """
        Class for parsing intersections from Redis to Neo4j.
        :param geo_connector:
        """
        self.geo_connector = geo_connector

    async def parse_to_graph(self):
        geo_connector = GeoConnector()

        client = geo_connector.mongo_db()
        db = client.geo_data
        collection: pymongo.collection.Collection = db.paths

        # ensure index creation
        start_node_end_node_index = collection.create_index(
            [
                ("start_node", ASCENDING),  # Use ASCENDING or DESCENDING based on your need
                ("end_node", ASCENDING),  # You can use DESCENDING if you need a descending order index
            ]
        )
        total = geo_connector.mongo_db().get_database("geo_data").get_collection("intersections").count_documents({})
        red = geo_connector.redis_db()
        addressed_nodes = []
        cursor = "0"
        while cursor != 0:
            cursor, new_keys = red.scan(cursor, match="*", count=10000)
            addressed_nodes.extend(new_keys)

        print("Adding connections")
        print("Total nodes: " + str(total))
        print("Addressed nodes: " + str(len(addressed_nodes)))
        print("Unaddressed nodes: " + str(total - len(addressed_nodes)))
        print(f"Progress: {len(addressed_nodes) / total}")

        map_boundaries_search = MapBoundaries()

        def generate_query_for_lat_lon(parameters: tuple):
            query = {
                "lat": {"$gt": parameters[0], "$lt": parameters[1]},
                "lon": {"$gt": parameters[2], "$lt": parameters[3]},
            }
            return query

        if MULTI:
            GRID = 28
        else:
            GRID = 1

        proto_queries_list = map_boundaries_search.generate_grid_queries(GRID)

        queries = [[generate_query_for_lat_lon(pq) for pq in pql] for pql in proto_queries_list]

        print("Query iterations: " + str(len(queries)))
        for query in queries:
            print("Query iteration length: " + str(len(query)))

        self.process_queries(queries, multi=MULTI)

    def worker(self, query):
        self.parse_query_to_graph(query)

    def process_queries(self, queries, multi):
        # Define the worker function

        # Create a pool of worker processes
        if multi is True:
            # Use pool.map to run the worker function in parallel
            for query_batch in queries:
                pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)
                print(query_batch)
                pool.map(self.worker, query_batch)
                pool.close()
                pool.join()
        else:
            for query_batch in queries:
                for query in query_batch:
                    self.worker(query)

    def parse_query_to_graph(self, query: str):
        geo_connector = GeoConnector()
        api = overpy.Overpass(url=OVERPASS_API_URL)

        addressed_nodes = geo_connector.redis_db().keys()

        # For testing purposes
        to_delete = []
        node_index = 1
        i = 0

        client = geo_connector.mongo_db()
        intersections = client.get_database("geo_data").get_collection("intersections")
        paths = client.get_database("geo_data").get_collection("paths")

        client_secondary = geo_connector.mongo_db()
        mongo_query_highways: Collection = client_secondary.get_database("geo_data").get_collection("highways_helper")
        mongo_query_nodes: Collection = client_secondary.get_database("geo_data").get_collection("nodes_helper")

        map_boundary_nodes = list(intersections.find(query))

        total = len(map_boundary_nodes)

        for record in map_boundary_nodes:
            key = record["_id"]
            i += 1
            if str(key).encode() not in addressed_nodes:
                node_index += 1
                print(f"Querying node: {key}\t Progress: {i / total:.2%} P: {node_index} {self.time_print()}")

                query_result = self.find_ways_of_node(key, mongo_query_highways, mongo_query_nodes)
                pathways = self.find_connections(
                    query_result,
                    key,
                    [],
                    api,
                    collection_highways=mongo_query_highways,
                    collection_nodes=mongo_query_nodes,
                )

                for pathway in pathways:
                    two_way_relationship = pathway.give_two_way_relationship()
                    intersections_exists = self.intersection_exists(
                        [pathway.intersection_a.id, pathway.intersection_b.id]
                    )
                    if (
                        self.exists(pathway.intersection_a, pathway.intersection_b, paths) is False
                        and pathway.forward is True
                        and intersections_exists
                    ):
                        two_way_relationship[0]["valid"] = True
                        paths.insert_one(two_way_relationship[0])
                    if (
                        self.exists(pathway.intersection_b, pathway.intersection_a, paths) is False
                        and pathway.backward is True
                        and intersections_exists
                    ):
                        two_way_relationship[1]["valid"] = True
                        paths.insert_one(two_way_relationship[1])
                self.redis_db_addressed_set(key)
                print("Addded" + self.time_print())

            else:
                print("S", end=" ")
                to_delete.append(str(key).encode())
        client.close()
        client_secondary.close()

    def time_print(self):
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        return " [" + current_time + "] "

    def find_ways_of_node(self, node_id, collection_highways, collection_nodes):
        try:
            result_ways = collection_nodes.find_one({"_id": node_id})
            result = list(collection_highways.find({"_id": {"$in": result_ways["ways"]}}))
            result = replace_id(result)

            result = Result(result)
        except Exception as e:
            print(e)
            print("Error on find_ways_of_node")
            print(node_id)
            print(result_ways["ways"])
            raise e
        return result

    def find_forward_intersections(self, starting_node_index, node_ids, keys, starting_id):
        i = starting_node_index
        while i < len(node_ids):  # forward search
            if keys[i] is not None and node_ids[i] != starting_id:
                return i
            i += 1
        return -1

    def find_backward_intersections(self, starting_node_index, node_ids, keys, starting_id):
        i = starting_node_index
        while i >= 0:  # backward search
            if keys[i] is not None and node_ids[i] != starting_id:
                return i
            i -= 1
        return -1

    def mongo_node_count(self, node_ids):
        while True:
            try:
                if "all_nodes" not in globals():
                    global all_nodes
                    all_nodes = (
                        self.geo_connector.mongo_db()
                        .get_database("geo_data")
                        .get_collection("intersections")
                        .distinct("_id")
                    )
            except Exception as e:
                print(e)
                print("Error on (node_count")
                print(*node_ids)
                time.sleep(1)
            else:
                break
        return sum(x in node_ids for x in all_nodes)

    def redis_db_addressed_set(self, key):
        while True:
            try:
                self.geo_connector.redis_db().set(key, 1)
            except Exception as e:
                print(e)
                print("redis_db_addressed.set(key, len(pathways))")
                print(key)
                time.sleep(1)
            else:
                break

    def check_for_intersections(self, nodes):
        if "all_nodes" not in globals():
            global all_nodes
            all_nodes = (
                self.geo_connector.mongo_db().get_database("geo_data").get_collection("intersections").distinct("_id")
            )
        intersections = []
        for node in nodes:
            if node.id in all_nodes:
                intersections.append(True)
            else:
                intersections.append(None)
        return intersections

    def find_connections(
        self,
        query_way_result,
        starting_id,
        checked_ways,
        api: overpy.Overpass,
        depth=0,
        collection_highways=None,
        collection_nodes=None,
    ):
        way: overpy.Way
        pathways = []
        for way in query_way_result.ways:
            try:
                # If the path_type is right
                if way.id not in checked_ways:
                    checked_ways.append(way.id)  # Add to checked ways
                    success = False
                    while not success:
                        try:
                            nodes = way.nodes
                            success = True
                        except Exception as e:
                            print(e)
                            print("nodes = way.get_nodes(resolve_missing=True)")
                            time.sleep(1)

                    node_ids = list(map(lambda node: node.id, nodes))

                    if len(node_ids) > 1 and depth < 4:  # If any other node except for intersection is found
                        # print("Get mget keys")
                        while True:
                            try:
                                if "all_nodes" not in globals():
                                    global all_nodes
                                    all_nodes = (
                                        self.geo_connector.mongo_db()
                                        .get_database("geo_data")
                                        .get_collection("intersections")
                                        .distinct("_id")
                                    )
                                keys = self.check_for_intersections(nodes)
                            except Exception as e:
                                print(e)
                                print("keys = redis_db.mget(list(map(lambda node: node.id, nodes)))")
                                time.sleep(1)
                            else:
                                break
                        starting_node_index = node_ids.index(starting_id)
                        intersection_after = self.find_forward_intersections(
                            starting_node_index, node_ids, keys, starting_id
                        )
                        i_before_nodes = []
                        i_after_nodes = []
                        if starting_node_index != 0:
                            intersection_before = self.find_backward_intersections(
                                starting_node_index, node_ids, keys, starting_id
                            )
                            if intersection_before == -1:  # Not found before ?----x
                                i_before_nodes.extend(nodes[: starting_node_index + 1])
                                sub_query_ways = self.find_ways_of_node(
                                    node_ids[0], collection_highways, collection_nodes
                                )
                                if len(sub_query_ways.way_ids) > 1:
                                    result = self.find_connections(
                                        query_way_result=sub_query_ways,
                                        starting_id=node_ids[0],
                                        checked_ways=checked_ways,
                                        api=api,
                                        depth=depth + 1,
                                        collection_highways=collection_highways,
                                        collection_nodes=collection_nodes,
                                    )
                                    if result != None:
                                        # check if way.tags["surface"] exists if it does not set surface to Unknown
                                        if "surface" in way.tags:
                                            surface_type = way.tags["surface"]
                                        else:
                                            surface_type = "Unknown"

                                        pathway = Pathway()
                                        pathway.generate(
                                            i_before_nodes,
                                            path_type=way.tags["highway"],
                                            surface_type=surface_type,
                                            way=way,
                                        )
                                        if len(result) == 1:
                                            pathway = pathway + result[0]
                                            pathways.append(pathway)
                                    print("Išči dalje nazaj")
                            else:  # Found before y----x
                                i_before_nodes.extend(nodes[intersection_before : starting_node_index + 1])
                                if "surface" in way.tags:
                                    surface_type = way.tags["surface"]
                                else:
                                    surface_type = "Unknown"
                                pathway = Pathway()
                                pathway.generate(
                                    i_before_nodes, path_type=way.tags["highway"], surface_type=surface_type, way=way
                                )
                                pathways.append(pathway)
                                # OK
                        if starting_node_index != len(node_ids) - 1:
                            if intersection_after == -1:  # not found after x----?
                                i_after_nodes.extend(
                                    nodes[starting_node_index:]
                                )  # i_after_nodes.extend(nodes[starting_node_index - 1 :])
                                sub_query_ways = self.find_ways_of_node(
                                    node_ids[len(node_ids) - 1], collection_highways, collection_nodes
                                )
                                if len(sub_query_ways.way_ids) > 1:
                                    result = self.find_connections(
                                        query_way_result=sub_query_ways,
                                        starting_id=node_ids[len(node_ids) - 1],
                                        checked_ways=checked_ways,
                                        api=api,
                                        depth=depth + 1,
                                        collection_highways=collection_highways,
                                        collection_nodes=collection_nodes,
                                    )
                                    if result != None:
                                        if "surface" in way.tags:
                                            surface_type = way.tags["surface"]
                                        else:
                                            surface_type = "Unknown"

                                        pathway = Pathway()
                                        pathway.generate(
                                            i_after_nodes,
                                            path_type=way.tags["highway"],
                                            surface_type=surface_type,
                                            way=way,
                                        )
                                        if len(result) == 1:
                                            pathway = pathway + result[0]
                                            pathways.append(pathway)
                                    print("Išči dalje naprej")
                            else:  # found after x----y
                                i_after_nodes.extend(nodes[starting_node_index : intersection_after + 1])
                                if "surface" in way.tags:
                                    surface_type = way.tags["surface"]
                                else:
                                    surface_type = "Unknown"

                                pathway = Pathway()
                                pathway.generate(
                                    i_after_nodes, path_type=way.tags["highway"], surface_type=surface_type, way=way
                                )
                                pathways.append(pathway)
                                # OK
            except KeyError:
                print(f"Not a OSM highway! {way.id}")
        return pathways

    def intersection_exists(self, id: int | list[int]):
        if "all_nodes" not in globals():
            global all_nodes
            all_nodes = (
                self.geo_connector.mongo_db().get_database("geo_data").get_collection("intersections").distinct("_id")
            )
        if isinstance(id, int):
            return id in all_nodes
        elif isinstance(id, list):
            return all(x in all_nodes for x in id)

    def exists(self, node_a, node_b, paths: pymongo.collection) -> bool:
        """
        Check if a connection between two intersections already exists in the database.
        :param paths: MongoDB collection3
        :param node_a: Intersection A
        :param node_b: Intersection B
        :return: True if the connection exists, False otherwise
        """
        return paths.count_documents({"start_node": node_a.id, "end_node": node_b.id}) > 0


def replace_id(d):
    if isinstance(d, dict):
        for key in list(d.keys()):
            if key == "_id":
                d["id"] = d.pop("_id")
            if key in d:
                replace_id(d[key])
    elif isinstance(d, list):
        for item in d:
            replace_id(item)
    return d


class N:
    def __init__(self, data):
        self.id = data["id"]
        self.ways = data["ways"]
        self.lat = data["lat"]
        self.lon = data["lon"]
        self.tags = data["tags"]


class W:
    def __init__(self, data):
        self.id = data["id"]
        self.nodes = [N(node) for node in data["nodes"]]
        self.tags = data["tags"]


class Result:
    def __init__(self, data):
        self.ways = [W(way) for way in data]
        self.way_ids = [way.id for way in self.ways]
