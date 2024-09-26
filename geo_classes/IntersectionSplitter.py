from pymongo.collection import Collection
from pymongo.database import Database

from graph.IntersectionPathway import IntersectionPathway
from setup.Constants import MULTI
import multiprocessing


class IntersectionSplitter:
    def __init__(self, geo_connector):
        self.geo_connector = geo_connector
        self.progress_generate_nodes = 0
        self.limit = 1000
        pass

    async def split(self):
        print("Inserting new nodes into new collection and creating paths between them...")
        if MULTI is False:
            self.generate_nodes()
        else:
            await self.generate_nodes_multiprocess()

        print("Rerouting original paths...")
        self.reroute_original_paths()

    def reroute_original_paths(self):
        mongo = self.geo_connector.mongo_db()
        database = mongo.get_database("geo_data")
        collection_paths = database.get_collection("paths")

        update_pipeline = [
            {
                "$set": {
                    "start_node": {"$concat": [{"$toString": "$start_node"}, "_", {"$toString": "$_id"}]},
                    "end_node": {"$concat": [{"$toString": "$end_node"}, "_", {"$toString": "$_id"}]},
                }
            },
            {
                "$merge": {
                    "into": "paths_splitted",  # Target collection
                    "whenMatched": "replace",  # Replace the document if it exists
                    "whenNotMatched": "insert",  # Insert a new document if it doesn't exist
                }
            },
        ]
        # Apply the aggregation update
        collection_paths.aggregate(update_pipeline)

    async def generate_nodes_multiprocess(self):
        mongo = self.geo_connector.mongo_db()
        database: Database = mongo.get_database("geo_data")
        collection_intersections: Collection = database.get_collection("intersections")
        count = collection_intersections.count_documents({})
        skip = 0
        limit = self.limit
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)
        while skip < count:
            pool.apply_async(
                self.generate_nodes,
                args=(
                    skip,
                    limit,
                ),
            )
            skip += limit
        pool.close()
        pool.join()

    def generate_nodes(self, skip=None, limit=None):
        mongo = self.geo_connector.mongo_db()
        database: Database = mongo.get_database("geo_data")
        collection_paths: Collection = database.get_collection("paths")
        collection_intersections: Collection = database.get_collection("intersections")
        collection_new_intersections: Collection = database.get_collection("intersections_splitted")
        collection_new_paths: Collection = database.get_collection("paths_splitted")

        new_intersections = 0
        new_paths = 0

        index = 0
        if skip is None or limit is None:
            cursor = collection_intersections.find()
            count = collection_intersections.count_documents({})
        else:
            cursor = collection_intersections.find().skip(skip).limit(limit)
            count = limit
        for i in cursor:
            fromto_b = list(collection_paths.find({"end_node": i["_id"]}, {"_id": 1, "nodes": 1}))
            fromto_a = list(collection_paths.find({"start_node": i["_id"]}, {"_id": 1, "nodes": 1}))
            i["original_id"] = i["_id"]
            for ftb in fromto_b:
                i["_id"] = f"{i['original_id']}_{ftb['_id']}"
                i["start"] = False
                try:
                    collection_new_intersections.insert_one(i)
                    new_intersections += 1
                except Exception as e:
                    print(e)

            for fta in fromto_a:
                i["_id"] = f"{i['original_id']}_{fta['_id']}"
                i["start"] = True
                i["traffic_signals"] = False
                try:
                    collection_new_intersections.insert_one(i)
                    new_intersections += 1
                except Exception as e:
                    print(e)

            for ftb in fromto_b:
                for fta in fromto_a:
                    new_paths += 1
                    ip = IntersectionPathway(
                        start_node=f"{i['original_id']}_{ftb['_id']}", end_node=f"{i['original_id']}_{fta['_id']}"
                    )
                    ip.calculate_curviness(ftb["nodes"][-2:-1] + fta["nodes"][:2])
                    collection_new_paths.insert_one(ip.__dict__)
            index += 1
            if index % 1000 == 0 and skip is None:
                print(f"Progress generate nodes single: {index}/{count}", end="\r")
