from setup.Constants import MAXIMUM_LATITUDE, MINIMUM_LATITUDE, MAXIMUM_LONGITUDE, MINIMUM_LONGITUDE


class MapBoundaries:
    def __init__(
            self,
            max_lat=MAXIMUM_LATITUDE,
            min_lat=MINIMUM_LATITUDE,
            max_lon=MAXIMUM_LONGITUDE,
            min_lon=MINIMUM_LONGITUDE,
    ):
        """Tells the parser which area to parse"""
        self.max_lat = max_lat
        self.min_lat = min_lat
        self.max_lon = max_lon
        self.min_lon = min_lon

    def max_number_in_matrix(self, matrix):
        max_number = 0
        for i in range(len(matrix)):
            for j in range(len(matrix[i])):
                if matrix[i][j] > max_number:
                    max_number = matrix[i][j]
        return max_number
    def __fill_matrix(self, m, n: int | None = None):
        """ Based on the 'graph coloring' algorithm, fills a matrix with integers ensuring no two adjacent cells have the same integer.
            :param m: The number of rows in the matrix
            :param n: The number of columns in the matrix
        """
        if n is None:
            n = m
        iteration = [1, 2, 3, 4]

        # Create an m x n matrix initialized with zeros
        matrix = [[0] * n for _ in range(m)]

        # Assign iteration to the matrix ensuring no two adjacent cells have the same iteration number
        for i in range(m):
            for j in range(n):
                # Possible iteration for the current cell
                possible_iteration = set(iteration)

                # Check adjacent cells (up, down, left, right and diagonals)
                # and remove those iterations from possible iterations
                if i > 0:
                    possible_iteration.discard(matrix[i - 1][j])  # Up
                if j > 0:
                    possible_iteration.discard(matrix[i][j - 1])  # Left
                if i < m - 1:
                    possible_iteration.discard(matrix[i + 1][j])  # Down
                if j < n - 1:
                    possible_iteration.discard(matrix[i][j + 1])  # Right
                if i > 0 and j > 0:
                    possible_iteration.discard(matrix[i - 1][j - 1])  # Top-left diagonal
                if i > 0 and j < n - 1:
                    possible_iteration.discard(matrix[i - 1][j + 1])  # Top-right diagonal
                if i < m - 1 and j > 0:
                    possible_iteration.discard(matrix[i + 1][j - 1])  # Bottom-left diagonal
                if i < m - 1 and j < n - 1:
                    possible_iteration.discard(matrix[i + 1][j + 1])  # Bottom-right diagonal

                # Assign the first possible color
                matrix[i][j] = possible_iteration.pop()

        return matrix



    def generate_grid_queries(self, grid_size: int):
        """Generates a list of queries to be used, divided in grid like cells.
        :param grid_size: The number of cells to divide the area into, will work on more processors if grid size is higher than 2"""
        lat_step = (self.max_lat - self.min_lat) / grid_size
        lon_step = (self.max_lon - self.min_lon) / grid_size
        total_queries = []
        matrix = self.__fill_matrix(grid_size, grid_size)

        for i in range(grid_size):
            for j in range(grid_size):
                total_queries.append(
                    (
                        self.min_lat + i * lat_step,
                        self.min_lat + (i + 1) * lat_step,
                        self.min_lon + j * lon_step,
                        self.min_lon + (j + 1) * lon_step,
                    )
                )
        max_number = self.max_number_in_matrix(matrix)

        ordered_queries = []
        for i in range(1, max_number + 1):
            ordered_queries.append([])

        i = 0
        for row in matrix:
            for column in row:
                ordered_queries[column - 1].append(total_queries[i])
                i += 1

        print(f"Total queries: {len(total_queries)}")
        print("Processed order of queries:")
        for row in matrix:
            print(row)

        return ordered_queries

    def __str__(self):
        return (
            "Map Boundaries:\n"
            f" - Maximum Latitude: {self.max_lat}\n"
            f" - Minimum Latitude: {self.min_lat}\n"
            f" - Maximum Longitude: {self.max_lon}\n"
            f" - Minimum Longitude: {self.min_lon}"
        )
