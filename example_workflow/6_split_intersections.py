"""5. Split intersections."""
from geo_classes.IntersectionSplitter import IntersectionSplitter
from setup.GeoConnector import GeoConnector
import asyncio

geo_connector = GeoConnector()


async def main():
    print("Splitting intersections...")
    print("#" * 50)
    print(geo_connector)
    print("#" * 50)
    splitter = IntersectionSplitter(geo_connector)
    await splitter.split()


if __name__ == "__main__":
    asyncio.run(main())
