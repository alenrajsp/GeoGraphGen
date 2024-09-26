"""2. Transform intersections by adding elevation data, traffic signals and way_ids."""
from setup.GeoConnector import GeoConnector
from geo_classes.ProcessedIntersectionMongoParser import ProcessedIntersectionMongoParser
import asyncio


async def main():
    geo_connector = GeoConnector()
    print("Parsing Mongo DB intersection nodes and processing them back to Mongo database...")
    print("#" * 50)
    print(geo_connector)
    print("#" * 50)
    parser = ProcessedIntersectionMongoParser(geo_connector)
    await parser.parse_to_mongo()
    print("Parsing complete.")


if __name__ == "__main__":
    asyncio.run(main())
