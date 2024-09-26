# Description: Setup file for the project. It contains all the constants that are used in the project.

# Configure from here
OVERPASS_API_URL = "http://zabojnik.informatika.uni-mb.si:1099/api/interpreter"
OPEN_ELEVATION_API_URL = "http://zabojnik.informatika.uni-mb.si:8086/api/v1/lookup"
REDIS_DB_ADDRESS = "127.0.0.1"
REDIS_DB_PORT = 6379
REDIS_DB_PASSWORD = "REDISPASSWORD"

NEO4J_DB_ADDRESS = "bolt://localhost:7688"
NEO4J_DB_USERNAME = "neo4j"
NEO4J_DB_PASSWORD = "NEO4JPASSWORD"

MONGO_DB_ADDRESS = "localhost"
MONGO_DB_PORT = 27018
MONGO_DB_USERNAME = "MONGOPASSWORD"
MONGO_DB_PASSWORD = "MONGOPASSWORD"

# WHOLE SLOVENIA
MAXIMUM_LATITUDE = 46.9
MINIMUM_LATITUDE = 45.4
MAXIMUM_LONGITUDE = 16.6
MINIMUM_LONGITUDE = 13.6

# If you want the process to run on multiple cores, set MULTI to True else set it to False
MULTI = True


# End of configuration, leave the rest of the file as it is
