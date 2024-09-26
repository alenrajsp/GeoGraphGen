from collections import namedtuple
from setup.GeoConnector import GeoConnector
from setup.MapBoundaries import MapBoundaries
import concurrent
from concurrent.futures import ThreadPoolExecutor

Intersection = namedtuple("Intersection", ["id", "lat", "lon", "tags"])


class IntersectionMongoParser:
    def __init__(self, geo_connector: GeoConnector, map_boundaries: MapBoundaries):
        """
        Class for parsing intersections from Overpass API to MongoDB.
        :param geo_connector: A GeoConnector object for connecting to the Redis, Neo4j, MongoDB, Overpass API.
        :param map_boundaries: A MapBoundaries object for defining the boundaries of the map.
        """
        self.geo_connector = geo_connector
        self.map_boundaries = map_boundaries

    def save_to_mongo_db(self, nodes: list[Intersection]) -> None:
        """
        Save a list of intersection nodes to a MongoDB database.
        :param nodes: A list of Intersection named tuples, each representing an intersection with attributes id, lat
        (latitude), lon (longitude), and tags.
        :return:
        """
        collection = self.geo_connector.mongo_db().get_database("geo_data").get_collection("intersections")
        for node in nodes:
            node_dict = {
                "lat": node.lat,
                "lon": node.lon,
                "tags": node.tags,
            }
            collection.update_one(
                {"_id": node.id},  # Filter criteria
                {"$setOnInsert": node_dict},  # Insert document if it does not exist
                upsert=True,
            )

    def overpass_query(self, min_lat: float, min_lon: float, max_lat: float, max_lon: float) -> list[Intersection]:
        """
        Query Overpass API for intersections in a given area.
        :param min_lat: Minimum latitude
        :param min_lon: Minimum longitude
        :param max_lat: Maximum latitude
        :param max_lon: Maximum longitude
        :return: List of intersection nodes
        """
        q = f"""way["highway"]({str(min_lat)}, {str(min_lon)}, {str(max_lat)}, {str(max_lon)})->.streets; node(way_link.streets:3-)->.intersections; .intersections out;"""
        overpass_api = self.geo_connector.overpass_api()
        try:
            result = overpass_api.query(q)
        except Exception as e:
            print(f"Query failed: {e}")
            return []
        nodes = []
        for node in result.nodes:
            nodes.append(Intersection(id=node.id, lat=float(node.lat), lon=float(node.lon), tags=node.tags))
        return nodes

    def process_square(self, square, parser_square: float) -> None:
        """
        Process a square of the map by calling overpass_query() and save_to_db().
        :param square: coordinates of the square
        :param parser_square: coordinate step
        :return:
        """
        intersection_nodes = self.overpass_query(
            min_lat=square[0],
            max_lat=square[0] + parser_square,
            min_lon=square[1],
            max_lon=square[1] + parser_square,
        )
        self.save_to_mongo_db(intersection_nodes)

    def generate_coordinate_grid_for_parsing(self) -> list[list[tuple[float, float]]]:
        """
        Prepare a list of all squares to parse based on the squares that have already been parsed. :return:  A list
        of lists, where each inner list contains tuples of latitude and longitude coordinates of a square.
        """
        map_boundaries = MapBoundaries()
        parser_square = 0.03
        """Initial values for current_lat and current_lon are set to the minimum values of the map boundaries."""
        try:
            current_lat = float(self.geo_connector.redis_db().get("current_lat"))
            current_lon = float(self.geo_connector.redis_db().get("current_lon"))
        except Exception as e:
            print("lat / lon not in db")
            print(e)
            current_lat = map_boundaries.min_lat
            current_lon = map_boundaries.min_lon
        squares = []
        while current_lat < map_boundaries.max_lat + parser_square:
            lat_square = []
            while current_lon < map_boundaries.max_lon + parser_square:
                lat_square.append((current_lat, current_lon))
                current_lon += parser_square - 0.00010
            squares.append(lat_square)
            current_lon = map_boundaries.min_lon
            current_lat += parser_square - 0.00010
        return squares

    def parse_map(self) -> None:
        """Parse map and save intersections to MongoDB by calling save_to_db()"""
        map_boundaries = MapBoundaries()
        parser_square = 0.03
        squares = self.generate_coordinate_grid_for_parsing()

        with ThreadPoolExecutor(max_workers=8) as executor:  # Adjust the number of workers as needed
            for lat_square in squares:
                future_to_square = {
                    executor.submit(self.process_square, square, parser_square): square for square in lat_square
                }
                for future in concurrent.futures.as_completed(future_to_square):
                    square = future_to_square[future]
                    try:
                        future.result()
                    except Exception as exc:
                        print("%r generated an exception: %s" % (square, exc))
                self.geo_connector.redis_db().set("current_lat", lat_square[0][0])
        self.geo_connector.redis_db().delete("current_lat")
        self.geo_connector.redis_db().delete("current_lon")
