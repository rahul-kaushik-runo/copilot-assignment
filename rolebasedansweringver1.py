import json
import google.generativeai as genai
from pymongo import MongoClient
import time
from difflib import get_close_matches


with open('backend/schema.txt', 'r') as file:
    file_contents = file.read()

SCHEMAS_STR = file_contents


class NLToMongoDBQuerySystem:
    def __init__(self):
        # Configuration - replace with your actual values
        API_KEY = "AIzaSyDzq0RE9mmQR6ipTNu4AffCGU6u7FmXQ38"
        MONGODB_URI = "mongodb://localhost:27017"
        DB_NAME = "runo"

        # Initialize Gemini
        genai.configure(api_key=API_KEY)
        self.model = genai.GenerativeModel('gemini-2.5-flash')

        # Initialize MongoDB connection
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DB_NAME]
        self.query_history = []
        
        # Schema processing
        self.schema_summaries = self._create_schema_summaries()
        self.full_schemas = self._parse_full_schemas()

        # Value synonyms (manually maintained or learned)
        self.value_synonyms = {
            "state": {
                "TG": ["telangana", "telengana", "tg", "t'gana"],
                "MH": ["maharashtra", "mh"],
                "KA": ["karnataka", "ka"]
            }
        }
        self.role_config = {
            "admin": {
                "allowed_collections": [
                    "license", "email-interaction", "rechurn-log", "report-history",
                    "company", "api-key", "call-interaction", "customer-assign-log",
                    "transaction", "roles", "crm-interaction", "allocation",
                    "time-log", "user", "customer", "whatsapp-interaction",
                    "process", "report-agent-login", "sms-interaction",
                    "email-template", "whatsapp-template", "cloud-virtual-number",
                    "customer-details", "report-agent-disposition", "crm-field",
                    "rechurn-status", "recurring-interaction", "sms-template"
                ],
                "field_restrictions": {},
                "query_filters": {},
                "permissions": {
                    "maskCusPhone": False,
                    "webAllocation": True,
                    "callRecording": {"play": True, "download": True},
                    "callTracking": {
                        "personalCalls": True,
                        "popup": True,
                        "isTrackAllSim": False,
                        "mandatory": True
                    },
                    "customer": {"delete": True, "view": True, "edit": True},
                    "reports": {
                        "downloadReports": True,
                        "normal": {
                            "crm": True, "allocation": True, "customer": True,
                            "call": True, "login": True, "whatsapp": True,
                            "email": True, "followup": True, "sms": True
                        },
                        "advanced": {
                            "disposition": True, "login": True
                        }
                    }
                }
            },
            "user": {
                "allowed_collections": [
                    "call-interaction", "customer-assign-log", "time-log",
                    "whatsapp-interaction", "sms-interaction", "recurring-interaction"
                ],
                "field_restrictions": {
                    "call-interaction": ["customer.phoneNumber"],
                    "whatsapp-interaction": ["customer.phoneNumber"],
                    "sms-interaction": ["customer.phoneNumber"]
                },
                "query_filters": {
                    "call-interaction": {"user.name": "<<user_name>>"},
                    "whatsapp-interaction": {"user.name": "<<user_name>>"},
                    "sms-interaction": {"user.name": "<<user_name>>"},
                    "recurring-interaction": {"user.name": "<<user_name>>"}
                },
                "permissions": {
                    "maskCusPhone": False,
                    "webAllocation": False,
                    "callRecording": {"play": True, "download": False},
                    "callTracking": {
                        "personalCalls": True,
                        "popup": True,
                        "isTrackAllSim": False,
                        "mandatory": True
                    },
                    "customer": {"delete": False, "view": True, "edit": True},
                    "reports": {
                        "downloadReports": False,
                        "normal": {
                            "crm": False, "allocation": False, "customer": False,
                            "call": False, "login": False, "whatsapp": False,
                            "email": False, "followup": False, "sms": False
                        },
                        "advanced": {
                            "disposition": False, "login": False
                        }
                    }
                }
            }
        }
        
        
    def _apply_role_constraints(self, query_text, collection_name, user_role, user_context):
        """Apply role-based constraints to queries and collections"""
        if user_role not in self.role_config:
            raise ValueError(f"Unknown role: {user_role}")
            
        role_config = self.role_config[user_role]
        
        # 1. Check if collection is allowed
        if collection_name not in role_config["allowed_collections"]:
            raise PermissionError(f"Role '{user_role}' cannot access collection '{collection_name}'")
        
        # 2. Check if query contains personal pronouns and apply user context
        query_text = self._apply_personal_context(query_text, user_context)
        
        # 3. Apply role-based query filters
        query_filters = role_config["query_filters"].get(collection_name, {})
        if query_filters:
            # Replace placeholders with actual user context
            for field, value in query_filters.items():
                if isinstance(value, str) and value.startswith("<<") and value.endswith(">>"):
                    context_key = value[2:-2]
                    if context_key in user_context:
                        query_filters[field] = user_context[context_key]
            
            # Modify the query text to include these filters
            query_text = self._modify_query_with_filters(query_text, query_filters)
            
        return query_text
    
    def _apply_personal_context(self, query_text, user_context):
        """Apply personal context when user uses personal pronouns like 'I', 'my', 'me'"""
        if not user_context.get("user_name"):
            return query_text
            
        # Define personal pronouns and their replacements
        personal_patterns = [
            ("how many calls did i make", f"how many calls did {user_context['user_name']} make"),
            ("my calls", f"calls by {user_context['user_name']}"),
            ("my interactions", f"interactions by {user_context['user_name']}"),
            ("my performance", f"performance of {user_context['user_name']}"),
            ("calls i made", f"calls made by {user_context['user_name']}"),
            ("interactions i had", f"interactions by {user_context['user_name']}"),
            ("my transactions", f"transactions by {user_context['user_name']}"),
            ("my customers", f"customers assigned to {user_context['user_name']}"),
            ("customers assigned to me", f"customers assigned to {user_context['user_name']}"),
            ("my allocation", f"allocation for {user_context['user_name']}"),
            ("my time log", f"time log for {user_context['user_name']}"),
            ("my login", f"login records for {user_context['user_name']}"),
            ("when did i", f"when did {user_context['user_name']}"),
            (" i ", f" {user_context['user_name']} "),
            (" me ", f" {user_context['user_name']} "),
            (" my ", f" {user_context['user_name']}'s "),
        ]
        
        modified_query = query_text.lower()
        
        # Apply replacements
        for pattern, replacement in personal_patterns:
            if pattern in modified_query:
                modified_query = modified_query.replace(pattern, replacement)
                print(f"Applied personal context: '{query_text}' → '{modified_query}'")
                break
        
        return modified_query
    
    def _modify_query_with_filters(self, query_text, filters):
        """Modify natural language query to include role-based filters"""
        if not filters:
            return query_text
            
        # Convert filters to natural language
        filter_descriptions = []
        for field, value in filters.items():
            if field == "user.name":
                filter_descriptions.append(f"for user {value}")
            else:
                filter_descriptions.append(f"where {field} is {value}")
                
        if filter_descriptions:
            return f"{query_text} {' and '.join(filter_descriptions)}"
        return query_text
    
    
    
    
    def _apply_field_restrictions(self, mongo_query, collection_name, user_role):
        """Remove restricted fields from projections"""
        if user_role not in self.role_config:
            return mongo_query
            
        role_config = self.role_config[user_role]
        restricted_fields = role_config["field_restrictions"].get(collection_name, [])
        
        if not restricted_fields:
            return mongo_query
            
        # Handle different query formats
        if isinstance(mongo_query, list):
            # Aggregation pipeline
            modified_pipeline = []
            for stage in mongo_query:
                if "$project" in stage:
                    # Remove restricted fields from projections
                    for field in restricted_fields:
                        stage["$project"].pop(field, None)
                modified_pipeline.append(stage)
            return modified_pipeline
        elif isinstance(mongo_query, dict):
            # Simple find query
            if "projection" in mongo_query:
                for field in restricted_fields:
                    mongo_query["projection"].pop(field, None)
            return mongo_query
            
        return mongo_query

    def _create_schema_summaries(self):
        """Create concise one-line summaries of each collection"""
        summaries = {}
        collections = SCHEMAS_STR.strip().split("Collection: ")[1:]
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            first_line = coll_text[name_end+1:].split('\n')[0].strip()
            summaries[collection_name] = first_line
        return summaries
    
    def _parse_full_schemas(self):
        """Parse full schemas into a dictionary for quick access"""
        schemas = {}
        collections = SCHEMAS_STR.strip().split("Collection: ")[1:]
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            schemas[collection_name] = f"collection: {collection_name}\n{coll_text[name_end+1:]}"
        return schemas
    
    def normalize_query(self, query):
        """Normalize natural language query using value synonyms"""
        normalized = query.lower()
        for field, synonym_map in self.value_synonyms.items():
            for canonical, variants in synonym_map.items():
                for variant in variants:
                    if variant.lower() in normalized:
                        normalized = normalized.replace(variant.lower(), canonical.lower())
        return normalized

    def _analyze_query_complexity(self, query_text):
        """Phase 0: Determine if query requires single or multiple collections"""
        normalized_query = self.normalize_query(query_text)
        
        prompt = f"""
You are a MongoDB expert assistant. Analyze this user query to determine if it requires data from ONE collection or MULTIPLE collections.

USER QUERY: "{normalized_query}"

AVAILABLE COLLECTIONS:
{self._format_collection_summaries()}

ANALYSIS INSTRUCTIONS:
1. Look for queries that ask for relationships, comparisons, or joins between different data types
2. Look for queries that need data from multiple domains (e.g., agent performance AND call history)
3. Look for queries with "and", "with", "combined with", "along with" that suggest multiple data sources
4. Look for queries asking for correlations, ratios, or calculations across different data types

IMPORTANT: Only suggest MULTIPLE collections if the query EXPLICITLY requires combining data from different sources.
Simple queries that can be answered from one collection should be marked as SINGLE.

Respond with EXACTLY ONE of these formats:
- "SINGLE: <collection_name>" - if query needs only one collection
- "MULTIPLE: <collection1>, <collection2>" - if query needs exactly two collections
- "MULTIPLE: <collection1>, <collection2>, <collection3>" - if query needs three or more collections

EXAMPLES:
- "show me all agents" → "SINGLE: agents"
- "agents with their call history" → "MULTIPLE: agents, call-history"
- "performance metrics for agents handling inbound calls" → "MULTIPLE: agent-performance, call-disposition"
- "count total calls" → "SINGLE: calls"
- "agent performance data" → "SINGLE: agent-performance"

RESPONSE:"""

        try:
            response = self.model.generate_content(prompt)
            response_text = response.text.strip()
            if response_text.startswith("SINGLE:"):
                collection = response_text.split("SINGLE:")[1].strip()
                return {"type": "single", "collections": [collection]}
            elif response_text.startswith("MULTIPLE:"):
                collections_str = response_text.split("MULTIPLE:")[1].strip()
                collections = [c.strip() for c in collections_str.split(",")]
                return {"type": "multiple", "collections": collections}
            else:
                # Fallback to single collection analysis
                return {"type": "single", "collections": [self._select_best_collection(query_text)]}
        except Exception as e:
            print(f"Error analyzing query complexity: {str(e)}")
            return {"type": "single", "collections": [self._select_best_collection(query_text)]}

    def _select_best_collection(self, query_text):
        """Phase 1: Have LLM select the best collection based on summaries"""
        normalized_query = self.normalize_query(query_text)
        
        print("normalized_query", normalized_query)
        
        prompt = f"""
You are a MongoDB expert assistant. Given the following collection summaries, 
select the SINGLE most appropriate collection for this query:

USER QUERY: "{normalized_query}"

AVAILABLE COLLECTIONS:
{self._format_collection_summaries()}

INSTRUCTIONS:
1. Analyze the user's query intent
2. Compare with each collection's purpose
3. Select ONLY ONE collection name that best matches
4. Respond ONLY with the collection name in this format: "collection: <name>"

EXAMPLE RESPONSES:
- "collection: report-agent-disposition"
- "collection: report-history"
"""
        try:
            response = self.model.generate_content(prompt)
            # Extract collection name from response
            if "collection:" in response.text.lower():
                return response.text.split("collection:")[1].strip().split()[0].strip('"\'')
            return None
        except Exception as e:
            print(f"Error parsing collection selection: {str(e)}")
            return None
            
    def _format_collection_summaries(self):
        """Format collection summaries for selection prompt"""
        return "\n".join(
            f"- {name}: {summary}" 
            for name, summary in self.schema_summaries.items()
        )
        
    def _generate_query_for_collection(self, query_text, collection_name):
        """Phase 2: Generate query for specific collection"""
        normalized_query = self.normalize_query(query_text)
        schema = self.full_schemas.get(collection_name)
        if not schema:
            return {"error": f"Collection {collection_name} not found"}
            
        # Prepare value synonyms string
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" 
                                    for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])
        print(schema)
            
        prompt = f"""
You are a MongoDB query expert. You think step by step before generating any query and you understand natural language queries very well. Given this collection schema, generate a query for:

USER QUERY: "{normalized_query}"

COLLECTION SCHEMA:
{schema}

VALUE SYNONYMS TO CONSIDER (use canonical values in query):
{value_synonyms_str}

Respond ONLY with a JSON object: {{ "collection": "{collection_name}", "query": <mongo_query> }}

CRITICAL RULES:
1. Use proper MongoDB query syntax
2. For text matching, use case-insensitive regex: {{ "$regex": "pattern", "$options": "i" }}
3. For EXACT text matching, use case-insensitive regex with ^ and $ anchors: {{ "$regex": "^pattern$", "$options": "i" }}
4. For partial matching (if explicitly requested), use: {{ "$regex": "pattern", "$options": "i" }}
5. For counting, use aggregation pipeline with $group
6. NEVER use invalid top-level operators like $count, $sum
7. Match query terms to schema fields exactly
8. For state queries, consider both full names and abbreviations using $or
9. Generate efficient queries, dont generate slow queries
10. Generate queries about only whats required, dont generate queries about everything, use proper filtering

BIGGEST RULE: Never generate any queries if the user is askin some random stuff, don't give any answer if the input is not related to the database, just say "I don't know" or "I can't help with that". IF the user types gibberish, just say "I don't know" or "I can't help with that". 

CRITICAL RULES - NEVER VIOLATE:
1. For simple queries, use find() format: {{ "field": "value" }}
2. For counting, use aggregation pipeline with $group: [{{ "$group": {{ "_id": null, "count": {{ "$sum": 1 }} }} }}]
3. For complex operations, use aggregation pipeline: [{{ "$match": {{ ... }} }}, {{ "$group": {{ ... }} }}]
4. NEVER use these invalid operators: $count, $sum as top-level, $avg as top-level
5. For case-insensitive text matching, use: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
6. NEVER nest $ operators under $in - this is invalid syntax
7. For case-insensitive matching with $in, use separate $or conditions instead

VALID QUERY FORMATS ONLY:
- Simple find: {{ "name": "john", "age": 25 }}
- Regex find: {{ "name": {{ "$regex": "john", "$options": "i" }} }}
- Range find: {{ "age": {{ "$gte": 18, "$lte": 65 }} }}
- Array find: {{ "tags": {{ "$in": ["tag1", "tag2"] }} }}
- Aggregation: [{{ "$match": {{ "status": "active" }} }}, {{ "$group": {{ "_id": "$category", "count": {{ "$sum": 1 }} }} }}]

EXAMPLES OF WHAT TO GENERATE:
- "find all users" → {{ "query": {{}} }}
- "count users" → {{ "query": [{{ "$group": {{ "_id": null, "count": {{ "$sum": 1 }} }} }}] }}
- "find users named john" → {{ "query": {{ "name": {{ "$regex": "john", "$options": "i" }} }} }}
- "users from telangana" → {{ "query": {{ "$or": [{{ "state": {{ "$regex": "telangana", "$options": "i" }} }}, {{ "state": {{ "$regex": "TG", "$options": "i" }} }}] }} }}

EXAMPLE OUTPUTS:
- {{ "collection": "report-agent-disposition", "query": {{ "callType": "inbound" }} }}
- {{ "collection": "report-history", "query": [{{ "$match": {{ "type": "agent-performance" }} }}] }}
"""
        try:
            response = self.model.generate_content(prompt)
            # Parse response
            response_text = response.text
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            return json.loads(response_text.strip().strip('"\''))
        except Exception as e:
            return {"error": f"Failed to parse query: {str(e)}", "raw_response": response.text}

    def natural_language_to_query(self, natural_language_query):
        """Main method to convert NL to MongoDB query"""
        # Phase 0: Analyze query complexity
        complexity_analysis = self._analyze_query_complexity(natural_language_query)
        
        if complexity_analysis["type"] == "single":
            # Single collection logic
            collection_name = complexity_analysis["collections"][0]
            if not collection_name:
                return {"error": "Could not determine appropriate collection"}
            
            query_result = self._generate_query_for_collection(natural_language_query, collection_name)
            if "error" in query_result:
                return query_result
                
            # Store successful query in history
            self.query_history.append((natural_language_query, query_result))
            if len(self.query_history) > 10:
                self.query_history.pop(0)
                
            return query_result
            
        else:  # Multiple collections - but we'll handle them differently
            # For now, let's combine the multi-collection results into a single response
            # that looks like the original format but includes combined data
            collections = complexity_analysis["collections"]
            
            # Generate individual queries for each collection
            all_queries = {}
            for collection_name in collections:
                query_result = self._generate_query_for_collection(natural_language_query, collection_name)
                if "error" not in query_result:
                    all_queries[collection_name] = query_result["query"]
            
            if not all_queries:
                return {"error": "Could not generate queries for any collection"}
            
            # Return a combined result that maintains backward compatibility
            primary_collection = collections[0]  # Use first collection as primary
            
            result = {
                "collection": primary_collection,  # Primary collection for backward compatibility
                "query": all_queries.get(primary_collection, {}),  # Primary query
                "multi_collection_data": {  # Additional data for multi-collection handling
                    "is_multi_collection": True,
                    "all_collections": collections,
                    "all_queries": all_queries
                }
            }
            
            # Store in history
            self.query_history.append((natural_language_query, result))
            if len(self.query_history) > 10:
                self.query_history.pop(0)
                
            return result
        
    def _convert_to_case_insensitive(self, query):
        """Convert query to case-insensitive version"""
        if not isinstance(query, dict):
            return query
            
        new_query = {}
        for key, value in query.items():
            if key.startswith("$"):
                # Handle operators like $and, $or
                new_query[key] = [self._convert_to_case_insensitive(v) for v in value] if isinstance(value, list) else value
            else:
                if isinstance(value, dict):
                    # Handle nested queries
                    new_query[key] = self._convert_to_case_insensitive(value)
                elif isinstance(value, str):
                    # Convert to case-insensitive regex match
                    new_query[key] = {"$regex": f"^{value}$", "$options": "i"}
                else:
                    new_query[key] = value
        return new_query

    def _get_operation_type(self, query):
        """Determine the MongoDB operation type from the query"""
        if isinstance(query, list):
            return "aggregate"
        for op_type in ["find", "insertOne", "insertMany", "updateOne", "updateMany", 
                        "deleteOne", "deleteMany", "countDocuments", "distinct"]:
            if op_type in query:
                return op_type
        return "find"
    
    def execute_query(self, collection_name, query):
        """Execute the generated MongoDB query"""
        try:
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            results = None
            
            if operation_type == "aggregate":
                pipeline = query if isinstance(query, list) else query.get("aggregate", [])
                # Process each $match stage for case-insensitive matching
                processed_pipeline = []
                for stage in pipeline:
                    if "$match" in stage:
                        stage["$match"] = self._convert_to_case_insensitive(stage["$match"])
                    processed_pipeline.append(stage)
                results = list(collection.aggregate(processed_pipeline, allowDiskUse=True))
            elif operation_type == "find":
                filter_query = query.get("find", query)
                filter_query = self._convert_to_case_insensitive(filter_query)
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

    def execute_multi_collection_query(self, query_result):
        """Execute multi-collection queries and combine results"""
        multi_data = query_result.get("multi_collection_data", {})
        if not multi_data.get("is_multi_collection", False):
            # Not a multi-collection query, execute normally
            collection_name = query_result["collection"]
            mongo_query = query_result["query"]
            return self.execute_query(collection_name, mongo_query)
        
        # Execute queries for all collections
        all_collections = multi_data["all_collections"]
        all_queries = multi_data["all_queries"]
        
        combined_results = []
        total_count = 0
        execution_details = {}
        
        for collection_name in all_collections:
            if collection_name in all_queries:
                query = all_queries[collection_name]
                result = self.execute_query(collection_name, query)
                
                if "error" not in result:
                    # Add collection identifier to each result
                    collection_results = result["results"]
                    for item in collection_results:
                        item["_source_collection"] = collection_name
                    
                    combined_results.extend(collection_results)
                    total_count += result["count"]
                    execution_details[collection_name] = {
                        "count": result["count"],
                        "query_type": result["query_type"]
                    }
                else:
                    execution_details[collection_name] = {"error": result["error"]}
        
        return {
            "results": combined_results,
            "count": total_count,
            "query_type": "multi_collection",
            "execution_details": execution_details,
            "collections_used": list(execution_details.keys())
        }
        
    def _generate_results_explanation(self, nl_query, mongo_query, results, collection_name, is_multi_collection=False, user_role=None):
        """Phase 3: Generate user-friendly results explanation"""
        result_count = results.get('count', 0)
        sample_results = results.get('results', [])[:3]
        query_type = results.get('query_type', 'find')
        role_context = f"""
USER ROLE: {user_role}
ROLE PERMISSIONS: {json.dumps(self.role_config.get(user_role, {}).get('permissions', {}), indent=2)}
"""
        
        
        
        # Handle multi-collection explanations
        if is_multi_collection:
            collections_used = results.get('collections_used', [])
            execution_details = results.get('execution_details', {})
            
            collections_summary = []
            for coll in collections_used:
                details = execution_details.get(coll, {})
                if "error" not in details:
                    collections_summary.append(f"{coll}: {details.get('count', 0)} results")
            
            multi_context = f"""
This query searched across multiple collections: {', '.join(collections_used)}
Results breakdown: {', '.join(collections_summary)}
Combined total: {result_count} results
"""
        else:
            multi_context = ""
        
        prompt = f"""
You are a helpful MongoDB assistant explaining query results in simple terms.

INSTRUCTIONS:
1. Say Hi 
2. Summarize the key findings from the results
3. Highlight any important numbers or patterns
4. Keep it concise (1-2 short paragraphs max)
5. Use natural, conversational language, but also keep it professional and simple and easy to understand
6. If no results found, suggest possible reasons
7. Tell it in a way that seems natural and human-like, not robotic, don't give detailed technical explanations
8. Avoid technical jargon, use simple terms, and keep it friendly
9. Don't talk about the query used or how it was generated, focus on the results and their meaning
10. Say the question the user asked in the beginning, in your own words, to show you understood it
{f"11. This query involved multiple data sources, explain how the data was combined" if is_multi_collection else ""}

# USER ORIGINAL QUESTION: "{nl_query}"

{role_context}

DATABASE COLLECTION: {collection_name}
QUERY TYPE: {query_type}
NUMBER OF RESULTS: {result_count}

{multi_context}

QUERY USED:
{json.dumps(mongo_query, indent=2)}

SAMPLE RESULTS (first 3):
{json.dumps(sample_results, indent=2, default=str)}
"""
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Unable to generate explanation: {str(e)}"

    def process_query(self, natural_language_query, user_role=None, user_context=None, include_explanation=True):
        """Modified process_query with role-based access control"""
        if not user_role:
            user_role = "user"  # Default to most restrictive role
        if not user_context:
            user_context = {}
            
        try:
            # Phase 0: Analyze query complexity with RBAC
            complexity = self._analyze_query_complexity(natural_language_query)
            
            # Filter collections by role
            allowed_collections = self.role_config.get(user_role, {}).get("allowed_collections", [])
            if complexity["type"] == "single":
                collection = complexity["collections"][0]
                if collection not in allowed_collections:
                    return {
                        "status": "error",
                        "message": f"You don't have permission to access '{collection}' data"
                    }
            else:
                # For multiple collections, check all are allowed
                unauthorized = [c for c in complexity["collections"] if c not in allowed_collections]
                if unauthorized:
                    return {
                        "status": "error",
                        "message": f"You don't have permission to access: {', '.join(unauthorized)}"
                    }
            
            # Apply role constraints to the query text for all collections
            if complexity["type"] == "single":
                modified_query = self._apply_role_constraints(
                    natural_language_query,
                    complexity["collections"][0],
                    user_role,
                    user_context
                )
            else:
                # For multi-collection queries, apply personal context to all
                modified_query = self._apply_personal_context(natural_language_query, user_context)
                # Then apply role constraints for each collection during query generation
                
            # Use self.natural_language_to_query instead of super()
            result = self.natural_language_to_query(modified_query)
            
            if "error" in result:
                return {
                    "status": "error",
                    "message": result["error"],
                    "role": user_role
                }
                
            collection_name = result["collection"]
            mongo_query = result["query"]
            
            # Apply field restrictions
            mongo_query = self._apply_field_restrictions(mongo_query, collection_name, user_role)
            
            # Execute query
            if result.get("multi_collection_data", {}).get("is_multi_collection", False):
                results = self.execute_multi_collection_query(result)
            else:
                results = self.execute_query(collection_name, mongo_query)
                
            # Build response
            response = {
                "status": "success" if "error" not in results else "error",
                "collection": collection_name,
                "generated_query": mongo_query,
                "role": user_role,
                "permissions": self.role_config[user_role]["permissions"]
            }
            
            if "error" in results:
                response["message"] = results["error"]
            else:
                response.update({
                    "results": results["results"],
                    "count": results["count"],
                    "query_type": results["query_type"]
                })
                
                # Generate explanation with role context
                if include_explanation:
                    response["explanation"] = self._generate_results_explanation(
                        natural_language_query,
                        mongo_query,
                        results,
                        collection_name,
                        is_multi_collection=result.get("multi_collection_data", {}).get("is_multi_collection", False),
                        user_role=user_role
                    )
            
            return response
            
        except PermissionError as e:
            return {
                "status": "error",
                "message": str(e),
                "role": user_role
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Query processing failed: {str(e)}",
                "role": user_role
            }


# Example usage function
def test_backward_compatibility():
    """Test function to ensure backward compatibility"""
    system = NLToMongoDBQuerySystem()
    
    test_queries = [
        "Show me all agents",  # Single collection
        "Get agents with their call history",  # Multi collection - but handled gracefully
        "Count total calls",  # Single collection
    ]
    
    print("Testing Backward Compatible Multi-Collection System")
    print("=" * 50)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTest {i}: {query}")
        print("-" * 30)
        
        try:
            result = system.process_query(query, include_explanation=True)
            
            # Check if response has the expected structure
            expected_fields = ["status", "collection", "generated_query"]
            has_expected_structure = all(field in result for field in expected_fields)
            
            print(f"✅ Has expected structure: {has_expected_structure}")
            print(f"Status: {result['status']}")
            print(f"Collection: {result.get('collection', 'N/A')}")
            print(f"Results count: {result.get('count', 0)}")
            
            # Check for multi-collection info
            if "multi_collection_info" in result:
                print(f"🔄 Multi-collection detected: {result['multi_collection_info']['all_collections']}")
            else:
                print("📄 Single collection query")
                
        except Exception as e:
            print(f"❌ Test failed with exception: {str(e)}")

if __name__ == "__main__":
    query_system = NLToMongoDBQuerySystem()

    # Test user-specific queries
    print("=== Testing User-Specific Queries ===")
    
    # Test 1: Admin asking about their own calls
    print("\n1. Admin asking 'how many calls did I make?'")
    admin_result = query_system.process_query(
        "",
        user_role="admin",
        user_context={"user_name": "Kiran Dev"}
    )
    print(f"Status: {admin_result['status']}")
    if admin_result['status'] == 'success':
        print(f"Collection: {admin_result['collection']}")
        print(f"Query: {admin_result['generated_query']}")
        print(f"Count: {admin_result.get('count', 0)}")
    else:
        print(f"Error: {admin_result.get('message', 'Unknown error')}")

    # Test 2: Regular user asking about their calls
    print("\n2. Regular user asking 'how many calls did I make?'")
    user_result = query_system.process_query(
        "how many calls did I make?",
        user_role="user",
        user_context={"user_name": "Aishwarya"}
    )
    print(f"Status: {user_result['status']}")
    if user_result['status'] == 'success':
        print(f"Collection: {user_result['collection']}")
        print(f"Query: {user_result['generated_query']}")
        print(f"Count: {user_result.get('count', 0)}")
    else:
        print(f"Error: {user_result.get('message', 'Unknown error')}")

    # Test 3: General customer count query (admin only)
    print("\n3. Admin asking 'how many customers are there in the database?'")
    customer_result = query_system.process_query(
        "how many customers are there in the database?",
        user_role="admin",
        user_context={"user_name": "Kiran Dev"}
    )
    print(f"Status: {customer_result['status']}")
    if customer_result['status'] == 'success':
        print(f"Collection: {customer_result['collection']}")
        print(f"Count: {customer_result.get('count', 0)}")
    else:
        print(f"Error: {customer_result.get('message', 'Unknown error')}")

    # Test 4: User trying to access restricted data
    print("\n4. Regular user trying to access customer data")
    restricted_result = query_system.process_query(
        "how many customers are there?",
        user_role="user",
        user_context={"user_name": "Kiran Dev"}
    )
    print(f"Status: {restricted_result['status']}")
    print(f"Message: {restricted_result.get('message', 'No message')}")