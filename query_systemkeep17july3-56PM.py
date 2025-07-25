import json
import google.generativeai as genai
from pymongo import MongoClient
import time
from difflib import get_close_matches

class NLToMongoDBQuerySystem:
    def __init__(self):
        API_KEY = "AIzaSyCYsPGc7kXhDteLjmkn2sXrk15q7nevHeY"
        MONGODB_URI = "mongodb://localhost:27017"
        DB_NAME = "runo"

        genai.configure(api_key=API_KEY)
        self.model = genai.GenerativeModel('gemini-2.0-flash')

        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DB_NAME]
        self.query_history = []

        self.collection_schemas = self._sample_all_collection_schemas()

        # 🔧 Value synonyms (manually maintained or learned)
        self.value_synonyms = {
            "state": {
                "TG": ["telangana", "telengana", "tg", "t'gana"],
                "MH": ["maharashtra", "mh"],
                "KA": ["karnataka", "ka"]
            }
        }

    def _sample_all_collection_schemas(self, sample_limit=1000):
        schemas = {}
        for collection_name in self.db.list_collection_names():
            collection = self.db[collection_name]
            all_fields = set()
            max_fields_count = 0
            sample_doc = None
            for doc in collection.find().limit(sample_limit):
                doc_fields = set(doc.keys())
                all_fields.update(doc_fields)
                if len(doc_fields) > max_fields_count:
                    max_fields_count = len(doc_fields)
                    sample_doc = doc
            schemas[collection_name] = {
                "fields": list(all_fields),
                "sample": sample_doc if sample_doc else {}
            }
        return schemas

    # 🔧 Normalize natural language query using value synonyms
    def normalize_query(self, query):
        normalized = query.lower()
        for field, synonym_map in self.value_synonyms.items():
            for canonical, variants in synonym_map.items():
                for variant in variants:
                    if variant.lower() in normalized:
                        normalized = normalized.replace(variant.lower(), canonical.lower())
        return normalized

    def fuzzy_match(self, input_str, valid_options, threshold=0.8):
        match = get_close_matches(input_str.lower(), [v.lower() for v in valid_options], n=1, cutoff=threshold)
        return match[0] if match else input_str

    def natural_language_to_query(self, natural_language_query, query_type="auto"):
        normalized_query = self.normalize_query(natural_language_query)

        # 🔧 Prepare schema string
        schemas_str = ""
        for cname, info in self.collection_schemas.items():
            schemas_str += f"\nCollection: {cname}\nFields: {info['fields']}\nSample: {json.dumps(info['sample'], default=str)}\n"

        # 🔧 Prompt history
        history_examples = ""
        if self.query_history:
            history_examples = "Recent successful queries:\n"
            for nl_query, mongo_query in self.query_history[-3:]:
                history_examples += f"- \"{nl_query}\" → {json.dumps(mongo_query)}\n"

        # 🔧 Collection names
        collections_list = ", ".join(self.collection_schemas.keys())

        # 🔧 Entity synonyms
        entity_synonyms = {
            "user": ["users", "user", "account", "profile"],
            "order": ["orders", "order", "purchase", "transaction"],
            "product": ["products", "product", "item", "goods"],
            "log": ["logs", "log", "event", "activity"]
        }
        entity_synonyms_str = "\n".join([f"{k}: {v}" for k, v in entity_synonyms.items()])

        # 🔧 Value synonyms string for Gemini
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])

        prompt = f"""
You are an extremely smart and helpful MongoDB expert. Given the schemas below, select the best collection for the user's query and generate a valid MongoDB query for it.
Respond ONLY with a JSON object: {{ "collection": <collection_name>, "query": <mongo_query> }}

Schemas:
{schemas_str}

Entity synonyms:
{entity_synonyms_str}

Value synonyms to consider (use canonical values in query):
{value_synonyms_str}



Collections available: {collections_list}

User query: "{normalized_query}"

Instructions:
- Match query terms to the best matching collection using schemas and synonyms.
- Normalize values using the value synonym list.
- Always prefer safety, clarity, and helpfulness.
- Run case insensitive queries, and use lowercase for all field values. 
- IF THE USER IS ASKING FOR SOME SPECIFIC STATE USE BOTH THE STATE CODE AND FULL NAME IN THE QUERY.
- If the query is ambiguous, ask for clarification.
- Do NOT generate harmful queries (like dropping the DB).
"""

        response = self.model.generate_content(prompt)
        try:
            response_text = response.text
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            response_text = response_text.strip().strip('"\'')
            result_json = json.loads(response_text)
            if "error" in result_json:
                return result_json
            self.query_history.append((natural_language_query, result_json))
            if len(self.query_history) > 10:
                self.query_history.pop(0)
            return result_json
        except Exception as e:
            return {"error": f"Failed to parse Gemini response: {str(e)}", "raw_response": response.text}

    def execute_query(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            results = None
            if operation_type == "aggregate":
                pipeline = query if isinstance(query, list) else query.get("aggregate", [])
                results = list(collection.aggregate(pipeline, allowDiskUse=True))
            elif operation_type == "find":
                filter_query = query.get("find", query)
                filter_query = self._lowercase_query_values(filter_query)
                cursor = collection.find(filter_query)
                results = list(cursor)
            else:
                return {"error": f"Unsupported operation type: {operation_type}"}
            return {
                "results": json.loads(json.dumps(results, default=str)),
                "count": len(results),
                "query_type": operation_type
            }
        except Exception as e:
            return {"error": str(e)}

    def _get_operation_type(self, query):
        if isinstance(query, list):
            return "aggregate"
        for op_type in ["find", "insertOne", "insertMany", "updateOne", "updateMany", 
                        "deleteOne", "deleteMany", "countDocuments", "distinct"]:
            if op_type in query:
                return op_type
        return "find"
    
    
    def _lowercase_query_values(self, query):
        if isinstance(query, dict):
            return {
                k: self._lowercase_query_values(v) 
                for k, v in query.items()
            }
        elif isinstance(query, list):
            return [self._lowercase_query_values(item) for item in query]
        elif isinstance(query, str):
            return query.lower()
        else:
            return query


    def process_query(self, natural_language_query, include_explanation=True):
        result = self.natural_language_to_query(natural_language_query)
        if "error" in result:
            return {
                "status": "error",
                "message": result["error"],
                "raw_response": result.get("raw_response", "")
            }
        collection_name = result["collection"]
        mongo_query = result["query"]
        print("collection used by ai: ", collection_name)
        results = self.execute_query(collection_name, mongo_query)
        response = {
            "status": "success" if "error" not in results else "error",
            "collection": collection_name,
            "generated_query": mongo_query,
        }
        if "error" in results:
            response["message"] = results["error"]
        else:
            response.update({
                "results": results["results"],
                "count": results["count"],
                "query_type": results["query_type"]
            })
            if include_explanation:
                explanation = self.generate_query_explanation(
                    natural_language_query, 
                    mongo_query, 
                    results
                )
                response["explanation"] = explanation
        return response

    def generate_query_explanation(self, natural_language_query, mongo_query, results):
        result_count = len(results["results"]) if "results" in results else 0
        result_sample = results["results"][:3] if "results" in results else []
        operation_type = results.get("query_type", "find")
        prompt = f"""
You are a helpful MongoDB Copilot assistant who works as a whatsapp chatbot. Explain the results of a database operation in a conversational, helpful manner.

Original natural language query: "{natural_language_query}"

MongoDB operation type: {operation_type}

MongoDB query executed: {json.dumps(mongo_query, indent=2)}

Number of results: {result_count}

Sample results (up to 3): {json.dumps(result_sample, indent=2, default=str)}
"""
        response = self.model.generate_content(prompt)
        return response.text
    
    
    
    
