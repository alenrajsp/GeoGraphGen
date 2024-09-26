from __future__ import annotations


class HillAscent:
    def __init__(
        self,
        flat_terrain=None,
        gentle_climb=None,
        moderate_climb=None,
        challenging_climb=None,
        steep_climb=None,
        extremely_steep_climb=None,
    ):
        """
        Object for storing hill ascent data (in meters).
        :param flat_terrain: distance of flat terrain
        :param gentle_climb: distance of gentle climb
        :param moderate_climb: distance of moderate climb
        :param challenging_climb: distance of challenging climb
        :param steep_climb: distance of steep climb
        :param extremely_steep_climb: distance of extremely steep climb
        """
        self.flat_terrain = flat_terrain if flat_terrain is not None else 0
        self.gentle_climb = gentle_climb if gentle_climb is not None else 0
        self.moderate_climb = moderate_climb if moderate_climb is not None else 0
        self.challenging_climb = (
            challenging_climb if challenging_climb is not None else 0
        )
        self.steep_climb = steep_climb if steep_climb is not None else 0
        self.extremely_steep_climb = (
            extremely_steep_climb if extremely_steep_climb is not None else 0
        )

    def __add__(self, other: HillAscent):
        return HillAscent(
            flat_terrain=self.flat_terrain + other.flat_terrain,
            gentle_climb=self.gentle_climb + other.gentle_climb,
            moderate_climb=self.moderate_climb + other.moderate_climb,
            challenging_climb=self.challenging_climb + other.challenging_climb,
            steep_climb=self.steep_climb + other.steep_climb,
            extremely_steep_climb=self.extremely_steep_climb
            + other.extremely_steep_climb,
        )

    def __sub__(self, other: HillAscent):
        return HillAscent(
            flat_terrain=self.flat_terrain - other.flat_terrain,
            gentle_climb=self.gentle_climb - other.gentle_climb,
            moderate_climb=self.moderate_climb - other.moderate_climb,
            challenging_climb=self.challenging_climb - other.challenging_climb,
            steep_climb=self.steep_climb - other.steep_climb,
            extremely_steep_climb=self.extremely_steep_climb
            - other.extremely_steep_climb,
        )


class HillAscentContainer:
    def __init__(self, forward: HillAscent = None, backward: HillAscent = None):
        """
        Object for storing hill ascent data (in meters) for forward and backward direction.
        :param forward: HillAscent object for forward direction
        :param backward: HillAscent object for backward direction
        """
        if forward is None:
            self.forward: HillAscent = HillAscent()
        else:
            self.forward: HillAscent = forward
        if backward is None:
            self.backward: HillAscent = HillAscent()
        else:
            self.backward: HillAscent = backward

    def add_to_ascent(self, distances: list[float], altitudes: list[float]):
        """
        Adds the distance and ascent to the appropriate category of object HillAscentContainer. 0-1% is flat, 1-3% is gentle, 3-6% is moderate,
        6-9% is challenging, 9-15% is steep, 15+% is extremely steep.
        :param distances:
        :param altitudes: list of altitudes
        :param distance: list of distances
        :param ascent: list of ascents
        :return: None
        """

        # Calculate gradient for each pair of points [0,1], [1,2], [2,3], ...
        for i in range(len(altitudes) - 1):
            altitude = altitudes[i + 1] - altitudes[i]
            distance = distances[i + 1] - distances[i]
            gradient = altitude / distance if distance != 0 else 0
            if gradient < -0.15:
                self.backward.extremely_steep_climb += distance
            elif gradient < -0.09:
                self.backward.steep_climb += distance
            elif gradient < -0.06:
                self.backward.challenging_climb += distance
            elif gradient < -0.03:
                self.backward.moderate_climb += distance
            elif gradient < -0.01:
                self.backward.gentle_climb += distance
            elif gradient < 0.01:
                self.forward.flat_terrain += distance
                self.backward.flat_terrain += distance
            elif gradient < 0.03:
                self.forward.gentle_climb += distance
            elif gradient < 0.06:
                self.forward.moderate_climb += distance
            elif gradient < 0.09:
                self.forward.challenging_climb += distance
            elif gradient < 0.15:
                self.forward.steep_climb += distance
            else:
                self.forward.extremely_steep_climb += distance

    def __add__(self, other: HillAscentContainer):
        return HillAscentContainer(
            forward=self.forward + other.forward,
            backward=self.backward + other.backward,
        )

    def __sub__(self, other: HillAscentContainer):
        return HillAscentContainer(
            forward=self.forward - other.forward,
            backward=self.backward - other.backward,
        )

    def add(self, other: HillAscentContainer):
        return self + other

    def add_reverse(self, other: HillAscentContainer):
        return HillAscentContainer(
            forward=self.forward + other.backward,
            backward=self.backward + other.forward,
        )
