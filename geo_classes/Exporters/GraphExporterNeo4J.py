import multiprocessing
import json
import os
from py2neo import Graph
from pymongo import MongoClient
from setup import GeoConnector
from setup.Constants import MULTI


class GraphExporterNeo4J:
    def __init__(self, geo_connector: GeoConnector, collection_nodes, collection_paths):
        self.geo_connector = geo_connector
        self.limit = 1000
        self.temp_dir = "database-docker\\neo4j_import\\temp_dir"
        self.collection_nodes = collection_nodes
        self.collection_paths = collection_paths

        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

        for file in os.listdir(self.temp_dir):
            file_path = os.path.join(self.temp_dir, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)

    def export_to_neo4j(self):
        if MULTI:
            self.export_to_neo4j_multi()
        else:
            self.export_to_neo4j_single()

    def export_to_neo4j_multi(self):
        # Generate intersection files in parallel
        count_intersections = (
            self.geo_connector.mongo_db()
            .get_database("geo_data")
            .get_collection(self.collection_nodes)
            .count_documents({})
        )
        skip = 0
        limit = self.limit
        process_id = 0
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        while skip < count_intersections:
            pool.apply_async(self.process_intersections, args=(skip, limit, process_id))
            skip += limit
            process_id += 1
        pool.close()
        pool.join()

        # Process intersection files sequentially
        self.process_files_sequentially("Intersection", "id")

        # Generate path files in parallel
        count_paths = (
            self.geo_connector.mongo_db()
            .get_database("geo_data")
            .get_collection(self.collection_paths)
            .count_documents({})
        )
        skip = 0
        process_id = 0
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)
        while skip < count_paths:
            pool.apply_async(self.process_paths, args=(skip, limit, process_id))
            skip += limit
            process_id += 1
        pool.close()
        pool.join()

        # Process path files sequentially
        self.process_files_sequentially("PATH_TO", "start_node", "end_node")

    def export_to_neo4j_single(self):
        # Process intersections
        count = (
            self.geo_connector.mongo_db()
            .get_database("geo_data")
            .get_collection(self.collection_nodes)
            .count_documents({})
        )
        skip = 0
        limit = self.limit
        process_id = 0
        while skip < count:
            self.process_intersections(skip=skip, limit=limit, process_id=process_id)
            skip += limit
            process_id += 1

        self.process_files_sequentially("Intersection", "id")

        # Process paths
        count = (
            self.geo_connector.mongo_db()
            .get_database("geo_data")
            .get_collection(self.collection_paths)
            .count_documents({})
        )
        skip = 0
        process_id = 0
        while skip < count:
            self.process_paths(skip=skip, limit=limit, process_id=process_id)
            skip += limit
            process_id += 1

        self.process_files_sequentially("PATH_TO", "start_node", "end_node")

    def process_intersections(self, skip=None, limit=None, process_id=None):
        mongo: MongoClient = self.geo_connector.mongo_db()
        database = mongo.get_database("geo_data")
        collection_intersections = database.get_collection(self.collection_nodes)

        if skip is not None and limit is not None:
            intersections = collection_intersections.find().skip(skip).limit(limit)
        else:
            intersections = collection_intersections.find()

        temp_file = os.path.join(self.temp_dir, f"intersections_{process_id}.json")

        with open(temp_file, "w") as f:
            batch = []
            for intersection in intersections:
                intersection_data = {
                    "id": str(intersection["_id"]),
                    "latitude": float(intersection["lat"]),
                    "longitude": float(intersection["lon"]),
                    "elevation": float(intersection["elevation"]),
                    "original_id": str(intersection["original_id"]),
                    "traffic_signals": int(intersection["traffic_signals"]),
                }
                batch.append(intersection_data)

                if len(batch) == self.limit:
                    json.dump(batch, f)
                    batch = []

            if batch:
                json.dump(batch, f)

        mongo.close()

    def process_paths(self, skip=None, limit=None, process_id=None):
        mongo: MongoClient = self.geo_connector.mongo_db()
        database = mongo.get_database("geo_data")
        collection_paths = database.get_collection(self.collection_paths)

        if skip is not None and limit is not None:
            paths = collection_paths.find({}, {"nodes": 0}).skip(skip).limit(limit)
        else:
            paths = collection_paths.find({}, {"node": 0})

        temp_file = os.path.join(self.temp_dir, f"paths_{process_id}.json")

        with open(temp_file, "w") as f:
            batch = []
            for path in paths:
                path_data = {
                    "start_node": str(path["start_node"]),
                    "end_node": str(path["end_node"]),
                    "distance": path["distance"],
                    "ascent": path["ascent"],
                    "descent": path["descent"],
                    "path_type": path["path_type"],
                    "surface": path["surface"],
                    "total_angle": path["total_angle"],
                    "curviness": path["curviness"],
                    "traffic_lights": path["traffic_lights"],
                    "hill_flat": path["hill_flat"],
                    "hill_gentle": path["hill_gentle"],
                    "hill_moderate": path["hill_moderate"],
                    "hill_challenging": path["hill_challenging"],
                    "hill_steep": path["hill_steep"],
                    "hill_extremely_steep": path["hill_extremely_steep"],
                    "bicycle_access": path["bicycle_access"],
                    "foot_access": path["foot_access"],
                    "car_access": path["car_access"],
                }
                batch.append(path_data)

                if len(batch) == self.limit:
                    json.dump(batch, f)
                    batch = []

            if batch:
                json.dump(batch, f)

        mongo.close()

    def process_files_sequentially(self, label, id_key, end_id_key=None):
        neo4j = self.geo_connector.neo4j_db()
        neo4j.run("CREATE INDEX intersections_id IF NOT EXISTS FOR (i:Intersection) ON (i.id)")
        print("Created index for intersections_id if not exists")

        files = sorted([f for f in os.listdir(self.temp_dir) if f.endswith(".json")])
        for temp_file in files:
            if label == "Intersection":
                self.run_apoc_query(temp_file, label, id_key)
            else:
                self.run_apoc_query(temp_file, label, id_key, end_id_key)

    def run_apoc_query(self, temp_file, label, id_key, end_id_key=None):
        neo4j: Graph = self.geo_connector.neo4j_db()

        if label == "Intersection":
            query = f"""
            CALL apoc.load.json("file:///temp_dir/{temp_file}") YIELD value AS row
            MERGE (i:Intersection {{id: row.id}})
            SET i.latitude = row.latitude,
                i.longitude = row.longitude,
                i.elevation = row.elevation,
                i.original_id = row.original_id,
                i.traffic_signals = row.traffic_signals
            """
        else:
            query = f"""
            CALL apoc.load.json("file:///temp_dir/{temp_file}") YIELD value AS row
            MATCH (start:Intersection {{id: row.{id_key}}})
            MATCH (end:Intersection {{id: row.{end_id_key}}})
            MERGE (start)-[r:PATH_TO]->(end)
            SET r.distance = row.distance,
                r.ascent = row.ascent,
                r.descent = row.descent,
                r.path_type = row.path_type,
                r.surface = row.surface,
                r.total_angle = row.total_angle,
                r.curviness = row.curviness,
                r.traffic_lights = row.traffic_lights,
                r.hill_flat = row.hill_flat,
                r.hill_gentle = row.hill_gentle,
                r.hill_moderate = row.hill_moderate,
                r.hill_challenging = row.hill_challenging,
                r.hill_steep = row.hill_steep,
                r.hill_extremely_steep = row.hill_extremely_steep,
                r.bicycle_access = row.bicycle_access,
                r.foot_access = row.foot_access,
                r.car_access = row.car_access
            """

        neo4j.run(query)
        os.remove(f"database-docker\\neo4j_import\\temp_dir\\{temp_file}")
