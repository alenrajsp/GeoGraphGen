from py2neo import Graph
from pymongo import MongoClient
import folium

from setup.GeoConnector import GeoConnector


class ShortestPathFinder:
    def __init__(self, geo_connector: GeoConnector):
        # Connect to Neo4j and MongoDB databases
        self.graph: Graph = geo_connector.neo4j_db()
        self.mongo: MongoClient = geo_connector.mongo_db()

    def find_shortest_path(self, start_node_id, end_node_id, relationship_type="PATH_TO", weight_property="distance"):
        query = f"""
        MATCH (start:Intersection {{id: '{start_node_id}'}}), (end:Intersection {{id: '{end_node_id}'}})
        CALL apoc.algo.dijkstra(start, end, '{relationship_type}', '{weight_property}') YIELD path, weight
        RETURN [node IN nodes(path) | node.id] AS path_ids, weight
        """

        result = self.graph.run(query).data()

        if result:
            return result[0]
        else:
            return None

    def get_path_details(self, path_ids):
        nodes_collection = self.mongo["geo_data"]["paths_splitted"]
        merged_nodes = []

        for i in range(len(path_ids) - 1):
            start_node = path_ids[i]
            end_node = path_ids[i + 1]

            path_segment = nodes_collection.find_one({"start_node": start_node, "end_node": end_node})

            if path_segment and "nodes" in path_segment:
                if not merged_nodes:
                    merged_nodes.extend(path_segment["nodes"])
                else:
                    merged_nodes.extend(path_segment["nodes"][1:])

        return merged_nodes

    def plot_path_on_map(self, merged_nodes):
        start_lat = merged_nodes[0]["lat"]
        start_lon = merged_nodes[0]["lon"]
        my_map = folium.Map(location=[start_lat, start_lon], zoom_start=14)

        coordinates = [(node["lat"], node["lon"]) for node in merged_nodes]

        folium.PolyLine(locations=coordinates, color="blue").add_to(my_map)

        for node in [merged_nodes[0]] + [merged_nodes[-1]]:
            folium.Marker(location=[node["lat"], node["lon"]], popup=f"Node ID: {node['id']}").add_to(my_map)

        my_map.save("path_map.html")

        return my_map


if __name__ == "__main__":
    geo_connector = GeoConnector()

    path_finder = ShortestPathFinder(geo_connector)
    start_node_id = "1390132474_66b9fcb793535ceba33f1b41"
    end_node_id = "5053982914_66bb3d1c694c7951072a9a4b"

    result = path_finder.find_shortest_path(start_node_id, end_node_id)

    if result:
        print("Shortest path (node IDs):", result["path_ids"])
        print("Total weight (distance):", result["weight"])

        path_details = path_finder.get_path_details(result["path_ids"])

        path_finder.plot_path_on_map(path_details)
        print("Map created and saved as 'path_map.html'")
    else:
        print("No path found between the specified nodes.")
