import overpy
import py2neo
import pymongo
import redis

from setup.Constants import (
    REDIS_DB_ADDRESS,
    REDIS_DB_PORT,
    REDIS_DB_PASSWORD,
    MONGO_DB_ADDRESS,
    MONGO_DB_PORT,
    MONGO_DB_USERNAME,
    MONGO_DB_PASSWORD,
    NEO4J_DB_ADDRESS,
    NEO4J_DB_USERNAME,
    NEO4J_DB_PASSWORD,
    OVERPASS_API_URL,
    OPEN_ELEVATION_API_URL,
)


class GeoConnector:
    def __init__(self):
        self.__connection_strings = {
            "redis_db": {
                "host": REDIS_DB_ADDRESS,
                "port": REDIS_DB_PORT,
                "password": REDIS_DB_PASSWORD,
            },
            "mongo_db": {
                "host": MONGO_DB_ADDRESS,
                "port": MONGO_DB_PORT,
                "username": MONGO_DB_USERNAME,
                "password": MONGO_DB_PASSWORD,
            },
            "neo4j_db": {
                "uri": NEO4J_DB_ADDRESS,
                "user": NEO4J_DB_USERNAME,
                "password": NEO4J_DB_PASSWORD,
            },
        }

    @staticmethod
    def redis_db() -> redis.client.Redis:
        """
        Get the Redis database object.s
        :return:
        """

        return redis.Redis(
            host=REDIS_DB_ADDRESS,
            port=REDIS_DB_PORT,
            password=REDIS_DB_PASSWORD,
            retry_on_timeout=True,
        )

    @staticmethod
    def mongo_db() -> pymongo.MongoClient:
        """
        Get the Mongo database object.
        :return:
        """
        return pymongo.MongoClient(
            host=MONGO_DB_ADDRESS,
            port=MONGO_DB_PORT,
            username=MONGO_DB_USERNAME,
            password=MONGO_DB_PASSWORD,
        )

    @staticmethod
    def neo4j_db() -> py2neo.database.Graph:
        """
        Get the Neo4j database object.
        :return:
        """
        return py2neo.Graph(
            uri=NEO4J_DB_ADDRESS,
            user=NEO4J_DB_USERNAME,
            password=NEO4J_DB_PASSWORD,
        )

    @staticmethod
    def overpass_api() -> overpy.Overpass:
        """
        Get the Overpass API object.
        :return:
        """
        return overpy.Overpass(url=OVERPASS_API_URL)

    @staticmethod
    def open_elevation_api() -> str:
        """
        Get the Open Elevation API object.
        :return:
        """
        return OPEN_ELEVATION_API_URL

    def __str__(self):
        return (
            f"Redis DB:\n"
            f" Host: {self.__connection_strings['redis_db']['host']}\n"
            f" Port: {self.__connection_strings['redis_db']['port']}\n"
            f" Password: {self.__connection_strings['redis_db']['password']}\n\n"
            f"Mongo DB:\n"
            f" Host: {self.__connection_strings['mongo_db']['host']}\n"
            f" Port: {self.__connection_strings['mongo_db']['port']}\n"
            f" Username: {self.__connection_strings['mongo_db']['username']}\n"
            f" Password: {self.__connection_strings['mongo_db']['password']}\n\n"
            f"Neo4j DB:\n"
            f" URI: {self.__connection_strings['neo4j_db']['uri']}\n"
            f" User: {self.__connection_strings['neo4j_db']['user']}\n"
            f" Password: {self.__connection_strings['neo4j_db']['password']}\n\n"
            f"Overpass API: {OVERPASS_API_URL} \n"
            f"Open Elevation API: {OPEN_ELEVATION_API_URL}"
        )
