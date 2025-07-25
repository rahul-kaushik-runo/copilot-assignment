from pymongo import MongoClient
from bson import ObjectId, Decimal128
from datetime import datetime
import json


def connect_to_mongodb(uri, db_name):
    """Connect to MongoDB and return the database object"""
    client = MongoClient(uri)
    return client[db_name]


def convert_for_display(value):
    """Convert BSON-specific types to JSON-friendly formats"""
    if isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, Decimal128):
        return float(value.to_decimal())
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, dict):
        return {k: convert_for_display(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [convert_for_display(v) for v in value]
    return value


def extract_schema_dot_notation(document, path=""):
    """Recursively extract schema using dot notation"""
    schema = {}

    if isinstance(document, dict):
        for key, value in document.items():
            full_path = f"{path}.{key}" if path else key
            if isinstance(value, dict):
                schema[full_path] = "dict"
                schema.update(extract_schema_dot_notation(value, full_path))
            elif isinstance(value, list):
                if value:
                    if isinstance(value[0], dict):
                        schema[full_path] = "Array[dict]"
                        schema.update(extract_schema_dot_notation(value[0], full_path))
                    else:
                        schema[full_path] = f"Array[{type(value[0]).__name__}]"
                else:
                    schema[full_path] = "Array"
            else:
                type_name = type(value).__name__
                if isinstance(value, ObjectId):
                    type_name = "ObjectId"
                elif isinstance(value, Decimal128):
                    type_name = "Decimal128"
                elif isinstance(value, datetime):
                    type_name = "datetime"
                schema[full_path] = type_name

    return schema


def analyze_collection(collection):
    """Analyze a collection and return the schema and the most complex sample document"""
    sample_doc = collection.find_one()
    if not sample_doc:
        return None, None

    max_fields = 0
    complex_doc = sample_doc

    for doc in collection.find().limit(100):
        field_count = len(extract_schema_dot_notation(doc))
        if field_count > max_fields:
            max_fields = field_count
            complex_doc = doc

    schema = extract_schema_dot_notation(complex_doc)
    return schema, complex_doc


def main():
    # MongoDB connection details
    mongodb_uri = "mongodb://localhost:27017/"
    database_name = "runo"

    try:
        db = connect_to_mongodb(mongodb_uri, database_name)

        for collection_name in db.list_collection_names():
            collection = db[collection_name]
            schema, document = analyze_collection(collection)

            if not schema:
                print(f"\nCollection '{collection_name}' is empty.")
                continue

            print(f"Collection: {collection_name}")
            print("Schema (dot notation):")
            print(json.dumps(schema, indent=2))
           
            print("\n=============================================================\n")
            #Append the result to a file
            with open("collection_schema.txt", "a") as file:
                file.write(f"Collection: {collection_name}\n")
                file.write("Schema (dot notation):\n")
                file.write(json.dumps(schema, indent=2) + "\n")
                file.write("\n=============================================================\n")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
