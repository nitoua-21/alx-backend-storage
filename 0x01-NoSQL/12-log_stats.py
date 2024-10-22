#!/usr/bin/env python3
"""
Script that provides statistics about Nginx logs stored in MongoDB
"""
from pymongo import MongoClient


def print_nginx_stats():
    """
    Connect to MongoDB and print statistics about Nginx logs
    """
    # Connect to MongoDB
    client = MongoClient('mongodb://127.0.0.1:27017')

    # Get the logs database and nginx collection
    collection = client.logs.nginx

    # Get total number of documents
    total_logs = collection.count_documents({})
    print(f"{total_logs} logs")

    # Print methods statistics
    print("Methods:")
    methods = ["GET", "POST", "PUT", "PATCH", "DELETE"]
    for method in methods:
        count = collection.count_documents({"method": method})
        print(f"    method {method}: {count}")

    # Get status check count
    status_check = collection.count_documents({
        "method": "GET",
        "path": "/status"
    })
    print(f"{status_check} status check")


if __name__ == "__main__":
    print_nginx_stats()
