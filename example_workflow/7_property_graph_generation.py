"""5. Split intersections."""
from geo_classes.Exporters.GraphExporterNeo4J import GraphExporterNeo4J
from setup.GeoConnector import GeoConnector
import asyncio

geo_connector = GeoConnector()


async def main():
    print("Exporting graph into Neo4j...")
    print("#" * 50)
    print(geo_connector)
    print("#" * 50)
    # If you want to export before splitting to simplify the process,
    # you can do it here, by using intersections and paths
    # instead of intersections_splitted and paths_splitted
    graph_exporter_neo4j = GraphExporterNeo4J(
        geo_connector, collection_nodes="intersections_splitted", collection_paths="paths_splitted"
    )
    graph_exporter_neo4j.export_to_neo4j()


if __name__ == "__main__":
    asyncio.run(main())
