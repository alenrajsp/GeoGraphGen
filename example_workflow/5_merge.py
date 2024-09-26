"""2. Generate identified connections to neo4j graph"""
from geo_classes.ConnectionMerger import ConnectionMerger
from setup.GeoConnector import GeoConnector
import asyncio
import tracemalloc

geo_connector = GeoConnector()


async def main():
    print("Merging unnecessary connections and removing nodes...")
    print("#" * 50)
    print(geo_connector)
    print("#" * 50)
    merger = ConnectionMerger(geo_connector)
    await merger.merge()
    merger.remove_unused_nodes()
    print("Merging complete.")


if __name__ == "__main__":
    tracemalloc.start()
    asyncio.run(main())
