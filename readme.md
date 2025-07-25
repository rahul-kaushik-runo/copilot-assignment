# Natural Language to MongoDB Query System

## Overview
This system converts natural language queries into MongoDB queries using Google Gemini AI and executes them on a MongoDB database. It allows users to interact with their database using simple English queries and returns structured responses along with natural language response.

## Features
- Converts natural language queries to MongoDB queries
- Retrieves schema information dynamically(RAG, basically)
- Validates and executes MongoDB queries efficiently(stops harmful queries (that might delete the entire database))
- Provides natural language response
- A simple UI for better demonstration

## Deployment 
The project is deployed on render, you can access it here: https://runodemon.onrender.com (sometimes due to inactivity it might be slow, so please be patient.)

## Dependencies
Install the required dependencies using:
```sh
pip install -r requirements.txt
```


## Configuration
Modify these values in `NLToMongoDBQuerySystem`:
```python
API_KEY = "your_gemini_api_key"  # Replace with your Gemini API key
MONGODB_URI = "mongodb://localhost:27017/"  # Replace with your MongoDB URI
DB_NAME = "whatsapp"  # Replace with your database name
COLLECTION_NAME = "messages"  # Replace with your collection name
```

## Running the system
``` python app.py ```

## Usage
### Example Queries:
#### 1. Find messages sent to Vikram Das
**Input:**
```python
query_system.process_query("Find messages sent to Vikram Das")
```
**Generated Query:**
```json
{"customer": "Vikram Das"}
```
**Output:**
```
Neha Kapoor sent a message to Vikram Das at 10:17 AM, saying: 'Let’s schedule a call.'
```

#### 2. Count messages by agent
**Input:**
```python
query_system.process_query("How many messages did each agent send?")
```
**Generated Query:**
```json
[
  {"$group": {"_id": "$agent", "count": {"$sum": 1}}}
]
```
**Output:**
```
Neha Kapoor sent 5 messages, Amit Verma sent 3 messages.
```

## API Endpoints
| Endpoint       | Method | Description                                                |
|-------------- |--------|------------------------------------------------------------|
| `/`           | GET    | Renders the main frontend page                             |
| `/api/query`  | POST   | Converts a natural language query into a MongoDB query and returns results |
| `/api/history`| GET    | Returns a list of previously processed queries            |
| `/api/schema` | GET    | Retrieves the schema and fields of the MongoDB collections |









