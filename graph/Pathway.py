from __future__ import annotations

import numpy as np
import overpy
from sport_activities_features.overpy_node_manipulation import OverpyNodesReader

from graph.PathwayHelpers.HillAscent import HillAscentContainer
from setup.Constants import OPEN_ELEVATION_API_URL
import time


class Pathway:
    def __init__(
        self,
        intersection_a=None,
        intersection_b=None,
        distance: float | None = None,
        type=None,
        surface=None,
        total_ascent: float | None = None,
        total_descent: float | None = None,
        nodes_list: None = None,
        traffic_lights: int | None = None,
        total_angle: float | None = None,  # Curviness helper for summing up roads
        curviness: float | None = None,  # Curviness helper for summing up roads
        forward: bool | None = True,
        backward: bool | None = True,
        bicycle_access: bool | None = True,
        foot_access: bool | None = True,
        car_access: bool | None = True,
    ):
        self.traffic_lights = traffic_lights
        self.intersection_a = intersection_a
        self.intersection_b = intersection_b
        self.distance = distance
        self.type = set([])
        self.surface = set([])
        self.total_ascent = total_ascent
        self.total_descent = total_descent
        self.total_angle = total_angle
        self.curviness = curviness
        self.nodes_list = nodes_list
        # Hill calculations variables
        self.hill_ascent_container = HillAscentContainer()
        self.forward = forward
        self.backward = backward
        self.bicycle_access = bicycle_access
        self.foot_access = foot_access
        self.car_access = car_access

    def road_type_check_access(self, way: overpy.Way):
        highway_type = way.tags.get("highway")

        if highway_type == "motorway":
            self.car_access = True
            self.bicycle_access = False
            self.foot_access = False
        elif highway_type == "trunk":
            self.car_access = True
            self.bicycle_access = (
                True  # Depending on local laws, usually not allowed, but OSM indicates "bicycle=yes" sometimes
            )
            self.foot_access = False
        elif highway_type == "primary":
            self.car_access = True
            self.bicycle_access = True
            self.foot_access = True  # Often allowed with restrictions, indicated by "foot=yes" or "foot=designated"
        elif highway_type == "secondary":
            self.car_access = True
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "tertiary":
            self.car_access = True
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "unclassified":
            self.car_access = True
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "residential":
            self.car_access = True
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "service":
            self.car_access = True
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "pedestrian":
            self.car_access = False
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "footway":
            self.car_access = False
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "cycleway":
            self.car_access = False
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "path":
            self.car_access = False
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "track":
            self.car_access = True  # Sometimes allowed for agricultural or forestry vehicles
            self.bicycle_access = True
            self.foot_access = True
        elif highway_type == "living_street":
            self.car_access = True
            self.bicycle_access = True
            self.foot_access = True
        else:
            # Default case, assumes access for all if the highway type is not recognized
            self.car_access = True
            self.bicycle_access = True
            self.foot_access = True

    def tag_type_check_access(self, way: overpy.Way):
        if way.tags.get("vehicle") == "no":
            self.car_access = False
            self.bicycle_access = False
        if way.tags.get("motor_vehicle") == "no":
            self.car_access = False
        if way.tags.get("motor_vehicle") == "yes":
            self.car_access = True
        if way.tags.get("bicycle") == "yes":
            self.bicycle_access = True
        if way.tags.get("foot") == "yes":
            self.foot_access = True
        if way.tags.get("motorcar") == "yes":
            self.car_access = True
        if way.tags.get("motorcar") == "no":
            self.car_access = False
        if way.tags.get("bicycle") == "no" or way.tags.get("bicycle") == "dismount":
            self.bicycle_access = False
        if way.tags.get("foot") == "no":
            self.foot_access = False
        if way.tags.get("access") == "no" or way.tags.get("access") == "private":
            self.car_access = False
            self.bicycle_access = False
            self.foot_access = False

    def generate(self, nodes, path_type=None, surface_type=None, way: overpy.Way = None):
        reader = OverpyNodesReader(open_elevation_api=OPEN_ELEVATION_API_URL)

        if way is not None:
            if way.tags.get("oneway") == "yes":
                self.backward = False
            self.road_type_check_access(way)
            self.tag_type_check_access(way)
            # Vehicle check

        while True:
            try:
                reader_nodes = reader.read_nodes(nodes)
            except Exception as e:
                print(e)
                print("reader_nodes = reader.read_nodes(nodes)")
                time.sleep(2)
            else:
                break
        while True:
            try:
                self.total_ascent = self.calculate_ascent(reader_nodes["altitudes"])
                self.total_descent = self.calculate_descent(reader_nodes["altitudes"])

            except Exception as e:
                print(e)
                print("hill_identificator.identify_hills()")
                time.sleep(2)
            else:
                break
        self.distance = reader_nodes["total_distance"]

        self.calculate_curviness(nodes)

        # Traffic lights count
        if len(nodes) > 2:
            self.traffic_lights = self.count_traffic_lights(nodes[1:-1])
        else:
            self.traffic_lights = 0

        hills = HillAscentContainer()
        hills.add_to_ascent(reader_nodes["distances"], reader_nodes["altitudes"])
        self.hill_ascent_container = hills

        self.intersection_a = nodes[0]
        self.intersection_b = nodes[-1]
        self.nodes_list = nodes
        self.type = set([path_type])
        self.surface = set([surface_type])

        # Curviness calculation

    def count_traffic_lights(self, nodes: list[overpy.Node]):
        count = 0
        for node in nodes:
            if "traffic_signals" in node.tags:
                count += 1
        return count

    def traffic_lights_in_intersection(self, intersection: overpy.Node):
        if "traffic_signals" in intersection.tags:
            return 1
        return 0

    def calculate_ascent(self, altitudes):
        ascent = 0
        for i in range(1, len(altitudes)):
            if altitudes[i] > altitudes[i - 1]:
                ascent += altitudes[i] - altitudes[i - 1]
        return ascent

    def calculate_descent(self, altitudes):
        descent = 0
        for i in range(1, len(altitudes)):
            if altitudes[i] < altitudes[i - 1]:
                descent += altitudes[i - 1] - altitudes[i]
        return descent

    def latlon_to_cartesian(self, lat, lon):
        R = 6371  # Earth radius in kilometers
        phi = np.radians(float(lat))
        theta = np.radians(float(lon))

        x = R * np.cos(phi) * np.cos(theta)
        y = R * np.cos(phi) * np.sin(theta)
        z = R * np.sin(phi)

        return np.array([x, y, z])

    # Function to calculate the angle between three points using the dot product
    def calculate_angle(self, p1, p2, p3):
        v1 = p2 - p1
        v2 = p3 - p2
        dot_product = np.dot(v1, v2)
        norm_product = np.linalg.norm(v1) * np.linalg.norm(v2)
        cos_angle = dot_product / norm_product
        cos_angle = np.clip(cos_angle, -1.0, 1.0)
        angle = np.arccos(cos_angle)

        return np.degrees(angle)

    def calculate_curviness(self, nodes):
        # map nodes array into lat lon touples
        nodes_map = [(node.lat, node.lon) for node in nodes]

        cartesian_nodes = [self.latlon_to_cartesian(lat, lon) for lat, lon in nodes_map]

        # Calculate angles and sum them
        angle = sum(
            self.calculate_angle(cartesian_nodes[i], cartesian_nodes[i + 1], cartesian_nodes[i + 2])
            for i in range(len(cartesian_nodes) - 2)
        )

        self.total_angle = angle
        self.curviness = angle / self.distance

    def to_mongo_helper_node(self, node):
        return {"id": node.id, "lat": float(node.lat), "lon": float(node.lon)}

    def give_two_way_relationship(self):
        node_a_id = self.nodes_list[0].id
        node_b_id = self.nodes_list[-1].id

        # Create forward relationship object
        relationship_a = {
            "start_node": node_a_id,
            "end_node": node_b_id,
            "distance": self.distance,
            "ascent": self.total_ascent,
            "descent": self.total_descent,
            "path_type": list(self.type),
            "surface": list(self.surface),
            "total_angle": self.total_angle,
            "curviness": self.curviness,
            "traffic_lights": self.traffic_lights,
            "hill_flat": self.hill_ascent_container.forward.flat_terrain,
            "hill_gentle": self.hill_ascent_container.forward.gentle_climb,
            "hill_moderate": self.hill_ascent_container.forward.moderate_climb,
            "hill_challenging": self.hill_ascent_container.forward.challenging_climb,
            "hill_steep": self.hill_ascent_container.forward.steep_climb,
            "hill_extremely_steep": self.hill_ascent_container.forward.extremely_steep_climb,
            "forward": self.forward,
            "backward": self.backward,
            "nodes": list(map(self.to_mongo_helper_node, self.nodes_list)),
            "bicycle_access": self.bicycle_access,
            "foot_access": self.foot_access,
            "car_access": self.car_access,
        }

        # Create backward relationship object
        relationship_b = {
            "start_node": node_b_id,
            "end_node": node_a_id,
            "distance": self.distance,
            "ascent": self.total_descent,
            "descent": self.total_ascent,
            "path_type": list(self.type),
            "surface": list(self.surface),
            "curviness": self.curviness,
            "traffic_lights": self.traffic_lights,
            "total_angle": self.total_angle,
            "hill_flat": self.hill_ascent_container.backward.flat_terrain,
            "hill_gentle": self.hill_ascent_container.backward.gentle_climb,
            "hill_moderate": self.hill_ascent_container.backward.moderate_climb,
            "hill_challenging": self.hill_ascent_container.backward.challenging_climb,
            "hill_steep": self.hill_ascent_container.backward.steep_climb,
            "hill_extremely_steep": self.hill_ascent_container.backward.extremely_steep_climb,
            "forward": self.backward,
            "backward": self.forward,
            "nodes": list(map(self.to_mongo_helper_node, self.nodes_list[::-1])),
            "bicycle_access": self.bicycle_access,
            "foot_access": self.foot_access,
            "car_access": self.car_access,
        }

        return [relationship_a, relationship_b]

    def __add__(self, second: Pathway):
        """
        Adds two Pathway objects.
        :param second: Pathway object (other). A-B and B-C -> A-C ...
        :return:
        """

        first = Pathway(
            intersection_a=self.intersection_a,
            intersection_b=self.intersection_b,
            distance=self.distance + second.distance,
            type=self.type.union(second.type),
            surface=self.surface.union(second.surface),
            total_ascent=self.total_ascent,
            total_descent=self.total_descent,
            nodes_list=self.nodes_list.copy(),
            traffic_lights=self.traffic_lights,
            total_angle=self.total_angle,
        )

        # Always same
        first.distance += second.distance
        first.traffic_lights += second.traffic_lights
        first.total_angle += second.total_angle
        first.curviness = first.total_angle / first.distance

        if second.bicycle_access == False:
            first.bicycle_access = False
        if second.foot_access == False:
            first.foot_access = False
        if second.car_access == False:
            first.car_access = False

        first.type.update(second.type)
        if first.intersection_b.id == second.intersection_a.id:  # A->B A'->B'
            first.traffic_lights += self.traffic_lights_in_intersection(second.intersection_a)
            first.total_ascent += second.total_ascent
            first.total_descent += second.total_descent
            first.intersection_b = second.intersection_b
            first.nodes_list = [*first.nodes_list, *second.nodes_list[1:]]  # Exclude common middle node
            first.hill_ascent_container += second.hill_ascent_container

        elif first.intersection_a.id == second.intersection_b.id:  # A'->B' A->B
            first.traffic_lights += self.traffic_lights_in_intersection(second.intersection_b)
            first.total_ascent += second.total_ascent
            first.total_descent += second.total_descent
            first.intersection_a = second.intersection_a
            first.nodes_list = [*second.nodes_list, *first.nodes_list[1:]]
            first.hill_ascent_container += second.hill_ascent_container

        elif first.intersection_b.id == second.intersection_b.id:  # A->B B'<-A'
            second.forward, second.backward = second.backward, second.forward  # switch forward and backward for second

            first.traffic_lights += self.traffic_lights_in_intersection(second.intersection_b)
            first.intersection_b = second.intersection_a
            first.total_ascent += second.total_descent
            first.total_descent += second.total_ascent
            combined_nodes = [*first.nodes_list, *second.nodes_list[::-1]]
            first.nodes_list = combined_nodes
            first.hill_ascent_container = first.hill_ascent_container.add_reverse(second.hill_ascent_container)

        elif first.intersection_a.id == second.intersection_a.id:  # B'<-A' A->B
            second.forward, second.backward = second.backward, second.forward  # switch forward and backward for second

            first.traffic_lights += self.traffic_lights_in_intersection(second.intersection_a)
            first.intersection_a = second.intersection_b
            first.total_ascent += second.total_descent
            first.total_descent += second.total_ascent
            combined_nodes = [*second.nodes_list[::-1], *first.nodes_list]
            first.nodes_list = combined_nodes
            first.hill_ascent_container = first.hill_ascent_container.add_reverse(second.hill_ascent_container)

        if self.forward is False or second.forward is False:
            first.forward = False
        if self.backward is False or second.backward is False:
            first.backward = False

        return first
