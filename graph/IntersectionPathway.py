import numpy as np
from bson import ObjectId


class IntersectionPathway:
    def __init__(
        self,
        start_node,
        end_node,
        distance=0.0,
        ascent=0,
        descent=0,
        path_type=None,
        surface=None,
        total_angle=0.0,
        curviness=0.0,
        traffic_lights=0,
        hill_flat=0.0,
        hill_gentle=0,
        hill_moderate=0,
        hill_challenging=0,
        hill_steep=0,
        hill_extremely_steep=0.0,
        forward=True,
        backward=False,
        nodes=None,
        bicycle_access=True,
        foot_access=True,
        car_access=True,
        valid=True,
        _id=None,
    ):
        if nodes is None:
            nodes = []
        if path_type is None:
            path_type = ["Intersection"]
        if surface is None:
            surface = ["Intersection"]
        self._id = _id or ObjectId()
        self.start_node = start_node
        self.end_node = end_node
        self.distance = distance
        self.ascent = ascent
        self.descent = descent
        self.path_type = path_type
        self.surface = surface
        self.total_angle = total_angle
        self.curviness = curviness
        self.traffic_lights = traffic_lights
        self.hill_flat = hill_flat
        self.hill_gentle = hill_gentle
        self.hill_moderate = hill_moderate
        self.hill_challenging = hill_challenging
        self.hill_steep = hill_steep
        self.hill_extremely_steep = hill_extremely_steep
        self.forward = forward
        self.backward = backward
        self.nodes = nodes if nodes is not None else []
        self.bicycle_access = bicycle_access
        self.foot_access = foot_access
        self.car_access = car_access
        self.valid = valid

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
        try:
            v1 = p2 - p1
            v2 = p3 - p2
            dot_product = np.dot(v1, v2)
            norm_product = np.linalg.norm(v1) * np.linalg.norm(v2)
            cos_angle = dot_product / norm_product
            cos_angle = np.clip(cos_angle, -1.0, 1.0)
            angle = np.arccos(cos_angle)
        except Exception:
            print("Error calculating angle")

        return np.degrees(angle)

    def calculate_curviness(self, nodes):
        # map nodes array into lat lon touples
        nodes_map = [(node["lat"], node["lon"]) for node in nodes]

        cartesian_nodes = [self.latlon_to_cartesian(lat, lon) for lat, lon in nodes_map]

        # Calculate angles and sum them
        angle = sum(
            self.calculate_angle(cartesian_nodes[i], cartesian_nodes[i + 1], cartesian_nodes[i + 2])
            for i in range(len(cartesian_nodes) - 2)
        )

        self.total_angle = angle
