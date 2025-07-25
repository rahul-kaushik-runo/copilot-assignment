from pymongo import MongoClient
from bson import ObjectId, Decimal128
from datetime import datetime
import json

def connect_to_mongodb(uri, db_name):
    """Connect to MongoDB and return database object"""
    client = MongoClient(uri)
    return client[db_name]

def convert_for_display(value):
    """Convert MongoDB-specific types to display-friendly formats"""
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

def analyze_document_structure(document, indent=0):
    """Analyze document structure with proper indentation for nested fields"""
    structure = {}
    
    if isinstance(document, dict):
        for key, value in document.items():
            if key == '_id' and isinstance(value, ObjectId):
                structure[f"{'  '*indent}{key}"] = "ObjectId"
            elif isinstance(value, dict):
                structure[f"{'  '*indent}{key}"] = "dict"
                nested = analyze_document_structure(value, indent+1)
                structure.update(nested)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                structure[f"{'  '*indent}{key}"] = "Array[dict]"
                nested = analyze_document_structure(value[0], indent+1)
                structure.update(nested)
            elif isinstance(value, list):
                type_name = f"Array[{type(value[0]).__name__}]" if value else "Array"
                structure[f"{'  '*indent}{key}"] = type_name
            else:
                type_name = type(value).__name__
                if isinstance(value, ObjectId):
                    type_name = "ObjectId"
                elif isinstance(value, Decimal128):
                    type_name = "Decimal128"
                elif isinstance(value, datetime):
                    type_name = "datetime"
                structure[f"{'  '*indent}{key}"] = type_name
    return structure

def format_document_content(document, indent=0):
    """Format document content with proper indentation"""
    lines = []
    document = convert_for_display(document)
    
    if isinstance(document, dict):
        for key, value in document.items():
            if isinstance(value, (dict, list)) and value:
                lines.append(f"{'  '*indent}{key}")
                lines.extend(format_document_content(value, indent+1))
            else:
                value_str = f'"{value}"' if isinstance(value, str) else json.dumps(value)
                lines.append(f"{'  '*indent}{key}")
                lines.append(f"{'  '*(indent+1)}{value_str}")
    elif isinstance(document, list) and document:
        for i, item in enumerate(document):
            lines.append(f"{'  '*indent}{i}")
            lines.extend(format_document_content(item, indent+1))
    
    return lines

def analyze_collection(collection):
    """Analyze a collection and return its schema and a sample document"""
    sample_doc = collection.find_one()
    if not sample_doc:
        return None, None
    
    # Find the most complex document in first 100 documents
    max_fields = 0
    complex_doc = sample_doc
    for doc in collection.find().limit(100):
        field_count = len(analyze_document_structure(doc))
        if field_count > max_fields:
            max_fields = field_count
            complex_doc = doc
    
    schema = analyze_document_structure(complex_doc)
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
                print(f"\nCollection '{collection_name}' is empty")
                continue
                
            print(f"\nCollection: {collection_name}")
            print("Fields and their data types:")
            for field, type_name in schema.items():
                print(f"{field}: {type_name}")
            print("==============================\n")
            # print("\nDocument content example:")
            # content_lines = format_document_content(document)
            # print("\n".join(content_lines))
            # print("\n" + "="*50 + "\n")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()