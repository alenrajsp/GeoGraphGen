import redis
import pymongo
import py2neo
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
)


class DatabaseResetter:
    @staticmethod
    def reset_redis_db():
        try:
            r = redis.Redis(host=REDIS_DB_ADDRESS, port=REDIS_DB_PORT, password=REDIS_DB_PASSWORD)
            r.flushall()
            print("Redis DB reset successfully.")
        except Exception as e:
            print(f"Failed to reset Redis DB: {e}")

    @staticmethod
    def reset_mongo_db():
        try:
            client = pymongo.MongoClient(
                host=MONGO_DB_ADDRESS, port=MONGO_DB_PORT, username=MONGO_DB_USERNAME, password=MONGO_DB_PASSWORD
            )
            client.drop_database("geo_data")
            print("Mongo DB reset successfully.")
        except Exception as e:
            print(f"Failed to reset Mongo DB: {e}")

    @staticmethod
    def reset_neo4j_db():
        try:
            graph = py2neo.Graph(uri=NEO4J_DB_ADDRESS, user=NEO4J_DB_USERNAME, password=NEO4J_DB_PASSWORD)
            graph.run("MATCH (n) DETACH DELETE n")
            print("Neo4j DB reset successfully.")
        except Exception as e:
            print(f"Failed to reset Neo4j DB: {e}")


def main():
    while True:
        print("\nSelect the database to reset:")
        print("1. Redis")
        print("2. MongoDB")
        print("3. Neo4j")
        print("4. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            DatabaseResetter.reset_redis_db()
        elif choice == "2":
            DatabaseResetter.reset_mongo_db()
        elif choice == "3":
            DatabaseResetter.reset_neo4j_db()
        elif choice == "4":
            print("Exiting.")
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
