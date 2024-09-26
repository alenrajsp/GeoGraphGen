from setup.GeoConnector import GeoConnector
import multiprocessing as mp
from sport_activities_features import ElevationIdentification
import asyncio
from pymongo.collection import Collection
from setup.Constants import MULTI
from pymongo import UpdateOne


class ProcessedIntersectionMongoParser:
    def __init__(self, geo_connector: GeoConnector):
        """
        Class for parsing intersections from MongoDB.
        :param geo_connector:
        """
        self.geo_connector = geo_connector
        self.BATCH_SIZE = 500

    async def parse_to_mongo(self):
        collection: Collection = self.geo_connector.mongo_db().get_database("geo_data").get_collection("intersections")
        total_documents = collection.count_documents({"elevation": {"$exists": False}})
        total_queries = (total_documents // self.BATCH_SIZE) + (1 if total_documents % self.BATCH_SIZE != 0 else 0)

        id_batches = []
        for i in range(total_queries):
            skip_count = i * self.BATCH_SIZE
            ids = list(
                collection.find({"elevation": {"$exists": False}}, {"_id": 1}).skip(skip_count).limit(self.BATCH_SIZE)
            )
            id_batches.append({"batch_id": i + 1, "total_batches": total_queries, "ids": [doc["_id"] for doc in ids]})

        print(f"Total documents: {total_documents}")
        print(f"Total queries: {total_queries}")

        if MULTI:
            pool = mp.Pool(mp.cpu_count())
            print("Adding nodes to MongoDB...")
            results = pool.map(self.generate_nodes_sync, id_batches)
            print("Done.")
        else:
            results = []
            for batch in id_batches:
                results.append(await self.generate_nodes(batch))

    async def find_ways_of_node(self, node, nodes_helper):
        # Optimized query to retrieve only way IDs
        q = f"""[out:json];node({node['_id']});way(bn);out ids;"""
        max_retries = 3

        for i in range(max_retries):
            try:
                query = nodes_helper.find_one({"_id": node["_id"]})["ways"]
                return query
            except Exception as e:
                print(f"Error in find_ways_of_node: {e}")
                print(f"Retrying... {i + 1}/{max_retries}")
                return None

    async def find_elevation_of_node(self, node):
        return await self.find_elevation_of_node([node])

    async def find_elevation_of_nodes(self, nodes):
        retry = 5
        for i in range(retry):
            try:
                elevation_identification = ElevationIdentification(
                    open_elevation_api=self.geo_connector.open_elevation_api(),
                    positions=[(node["lat"], node["lon"]) for node in nodes],
                )
                elevation = elevation_identification.fetch_elevation_data()
                return elevation
            except Exception as e:
                print(f"Error in find_elevation_of_nodes: {e}")
                print(f"Retrying... {i + 1}/{retry}")
                # wait for 3 seconds
                await asyncio.sleep(3)
        return -1000

    async def generate_node(self, node, elevation=None, nodes_helper: Collection = None):
        if elevation is None:
            elevation_promise = self.find_elevation_of_node(node)
        ways = self.find_ways_of_node(node, nodes_helper)  # async
        traffic_signals = check_traffic_signals(node)  # sync

        if elevation is None:
            elevation = await elevation_promise[0]

        ways_result = await ways

        node["elevation"] = elevation
        node["traffic_signals"] = traffic_signals
        node["way_ids"] = ways_result

        return node

    async def generate_nodes(self, batch):
        ids = batch["ids"]

        print(batch["batch_id"], "/", batch["total_batches"], "Processing", len(ids), "nodes")

        collection: Collection = self.geo_connector.mongo_db().get_database("geo_data").get_collection("intersections")
        nodes_helper: Collection = self.geo_connector.mongo_db().get_database("geo_data").get_collection("nodes_helper")

        nodes = list(collection.find({"_id": {"$in": ids}, "elevation": {"$exists": False}}))

        elevations = await self.find_elevation_of_nodes(nodes)  # async

        operations = []

        for i, node in enumerate(nodes):
            retries = 3
            for attempt in range(retries):
                try:
                    processed_node = await self.generate_node(node, elevation=elevations[i], nodes_helper=nodes_helper)
                    operations.append(UpdateOne({"_id": processed_node["_id"]}, {"$set": processed_node}, upsert=True))
                    # print(f'Added: {node["_id"]}')
                    break
                except Exception as e:
                    print(f"Error in add_nodes: {e}")
                    print(f"Retrying... {attempt + 1}/{retries}")
                    if attempt == retries - 1:
                        # Handle the failure case if all retries are exhausted
                        print(f"Failed to process node after {retries} retries: {node['_id']}")

        if operations:
            try:
                result = collection.bulk_write(operations, ordered=False)
                print(f"Bulk update completed with {result.modified_count} operations.")
            except Exception as e:
                print(f"Error in bulk_write: {e}")

        print(batch["batch_id"], "/", batch["total_batches"], "Completed", len(ids), "nodes")
        return [op._doc for op in operations if op._doc]

    def generate_nodes_sync(self, ids):
        return asyncio.run(self.generate_nodes(ids))

    def get_nodes_by_ids(self, ids):
        nodes = (
            self.geo_connector.mongo_db()
            .get_database("geo_data")
            .get_collection("intersections")
            .find({"_id": {"$in": ids}})
        )
        return [node for node in nodes]


def check_traffic_signals(node: dict):
    if "tags" in node and isinstance(node["tags"], dict):
        tags = node["tags"]
        if "highway" in tags and tags["highway"] == "traffic_signals":
            return True
    return False


# chunk into sizes of n
def chunks(array: list, n: int):
    for i in range(0, len(array), n):
        yield array[i : i + n]
