from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0")
collection = client["test_db"]["events"]

collection.insert_many([
    {"user_id": 1, "event": "login", "source": "web"},
    {"user_id": 2, "event": "logout", "source": "mobile"},
    {"user_id": 3, "event": "register", "source": "web"},
    {"user_id": 4, "event": "purchase", "source": "mobile"},
    {"user_id": 5, "event": "cancel", "source": "web"},
])
