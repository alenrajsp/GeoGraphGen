""" 1. Parse map and identify intersections. The script uses Overpass API to get the map data. """

from geo_classes.IntersectionMongoParser import IntersectionMongoParser
from setup.GeoConnector import GeoConnector
from setup.MapBoundaries import MapBoundaries

if __name__ == "__main__":
    map_boundaries = MapBoundaries()
    geo_connector = GeoConnector()
    print("Parsing map and saving intersections to Mongo database...")
    print("#" * 50)
    print(geo_connector)
    print("#" * 50)
    print(map_boundaries)
    print("#" * 50)

    parser = IntersectionMongoParser(geo_connector=geo_connector, map_boundaries=map_boundaries)
    parser.parse_map()
    print("Parsing complete.")
