# Instructions for creating a property graph from OpenStreetMap and DEM data

## Introduction
This repository contains the necessary scripts to create a property graph from OpenStreetMap and DEM data.

## Paper
This work will be published in a paper titled **"Novel method for representing complex geographical route properties used in sports training on a property graph"**.
TODO: Add paper reference here when available.

## Prerequisites
- Python 3.11>
- Self-hosted OpenElevation API instance ([instructions](https://open-elevation.com/#host-your-own)).
  - DEM dataset ([download @ OpenDem](https://opendem.info/opendemeu_download_4258.html))
- Self-hosted Overpass API instance ([instructions](https://wiki.openstreetmap.org/wiki/Overpass_API/Installation))
  - OpenStreetMap files ([download @ Geofabrik](https://download.geofabrik.de/))
- Docker (for running the databases)

## Creating the property graph
1. Clone the repository
2. Install the dependencies with [poetry](https://python-poetry.org/) - poetry install
3. Install / prepare the requirements and prepare the necessary EU-DEM (OpenElevation) and OpenStreetMap (Overpass) data
4. Setup the environment variables in the ```database-docker/docker-compose.yml``` folder
5. Setup the configuration file ```setup/Constants.py``` with the necessary information, such as connection strings and map parameters
6. Run the ```docker-compose.yml``` file to start the databases in ```database-docker``` folder
7. Run the python scripts in the example workflow in the following order:
    - ```0_OSM_preprocessing_prune.py``` - Prunes the OpenStreetMap data to only include the necessary information
    - ```1_OSM_preprocessing_helper_collections.py``` - Save the ways and nodes data from Overpass API to MongoDB
    - ```2_intersections_to_mongo_db.py``` - Identify the (road) intersections and save them to MongoDB using Overpass API
    - ```3_processed_intersections_to_mongo_db.py``` - Add elevation data and traffic signals to the intersections and save them to MongoDB
    - ```4_connections_to_mongo_db.py``` - Create the connections between the intersections and save them to MongoDB
    - ```5_merge.py``` - Iteratively merges unnecessary nodes and edges
    - ```6_split_intersections.py``` - Split the intersections into individual nodes for each road to ensure capturing angle data
    - ```7_property_graph_generation``` - Generate Neo4j property graph from the MongoDB data
    - ```8_example_path``` - Example for generating a sample path from the generated property graph
8. 🎉 You now have a working property graph in Neo4j! 🎉

### Mongo collections
The procedure followed generates the following Mongo collections
- **highways_helper** - Contains the ways data from OpenStreetMap of type **highway**
- **nodes_helper** - Contains the nodes data from OpenStreetMap of all ways of type **highway**
- **intersections** - Contains the nodes (intersections of the roads), before splitting
- **paths** - Contains the edges (paths) between the intersections, before splitting
- **intersections_splitted** - Contains the nodes (intersections of the roads), after splitting
- **paths_splitted** - Contains the edges (paths) between the intersections, after splitting

### Data model
The collections hold the following data upon completion:

#### Paths (paths_processed collection)
Each document in the `paths_processed` collection represents a road or path segment between two nodes (start and end nodes). It includes details about the physical characteristics of the path, elevation data, traffic controls, and accessibility for different modes of transportation. Here’s a breakdown of the key attributes:

- **_id**: Unique identifier for the path segment.
- **start_node**: The ID of the starting node of the path.
- **end_node**: The ID of the ending node of the path.
- **distance**: The length of the path in meters.
- **ascent**: The elevation gained along the path.
- **descent**: The elevation lost along the path.
- **path_type**: A list of classifications for the type of road/path (e.g., "residential").
- **surface**: Describes the surface type of the path (e.g., "asphalt," "gravel"). If unknown, it will be listed as "Unknown."
- **total_angle**: The cumulative angle of turns along the path.
- **curviness**: A measure of how curved the path is.
- **traffic_lights**: The number of traffic lights along the path.
- **hill_flat**: Percentage of the path that is flat.
- **hill_gentle**, **hill_moderate**, **hill_challenging**, **hill_steep**, **hill_extremely_steep**: These attributes describe the percentage of the path that falls into different hill categories based on steepness.
- **forward**: Indicates if the path can be traveled in the forward direction. (**always true**)
- **backward**: Indicates if the path can be traveled in the backward direction. (**true if reverse exists**)
- **nodes**: An array of nodes that make up the path. Each node contains:
  - **id**: Node ID.
  - **lat**: Latitude of the node.
  - **lon**: Longitude of the node.
- **bicycle_access**: Indicates whether bicycles are allowed on the path.
- **foot_access**: Indicates whether pedestrians are allowed on the path.
- **car_access**: Indicates whether cars are allowed on the path.

#### Nodes (intersections_processed collection)
Each document in the `intersections_processed` collection represents an intersection of roads, with additional data about location, elevation, traffic signals, and associated ways. Here's a breakdown of the key attributes:

- **_id**: Unique identifier for the intersection node.
- **lat**: Latitude of the intersection.
- **lon**: Longitude of the intersection.
- **tags**: A dictionary to store any additional metadata or tags of the node from OpenStreetMap.
- **elevation**: Elevation of the intersection point in meters.
- **traffic_signals**: A boolean flag indicating whether there are traffic signals at the intersection.
- **way_ids**: An array of IDs for the road segments (ways) that pass through this intersection.


