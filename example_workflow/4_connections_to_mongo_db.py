"""2. Generate identified connections to neo4j graph"""
from geo_classes.ConnectionMongoParser import ConnectionMongoParser
from setup.GeoConnector import GeoConnector
import asyncio

geo_connector = GeoConnector()


async def main():
    print("Generating connections into Mongo db...")
    print("#" * 50)
    print(geo_connector)
    print("#" * 50)
    parser = ConnectionMongoParser(geo_connector)
    await parser.parse_to_graph()
    print("Generating complete.")


if __name__ == "__main__":
    asyncio.run(main())
