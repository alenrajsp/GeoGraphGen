import time
from collections import defaultdict

from pymongo import MongoClient

from setup import GeoConnector


class StopWatch:
    def __init__(self):
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.end_time = time.time()

    def __str__(self):
        return str(self.end_time - self.start_time)


class DuplicatePathRemover:
    def __init__(self, geo_connector: GeoConnector):
        self.geo_connector: GeoConnector = geo_connector

    def identify_duplicates(self):
        mongo: MongoClient = self.geo_connector.mongo_db()
        mongo_paths = mongo["geo_data"]["paths"]

        timer = StopWatch()

        timer.start()
        path_dict = defaultdict(int)

        for path in mongo_paths.find({}, {"start_node": 1, "end_node": 1, "_id": 0}):
            key = f'{path["start_node"]}-{path["end_node"]}'
            path_dict[key] += 1

        timer.stop()
        print(f"Time taken (1/2) : {timer}")

        paths_with_duplicates = [key for key, count in path_dict.items() if count > 1]
        print(f"Number of paths with duplicates: {len(paths_with_duplicates)}")
        timer.stop()
        print(f"Duplicate path time taken (2/2) : {timer}")
