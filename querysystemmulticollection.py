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
        self.collection_relationships = self._define_collection_relationships()

        # Value synonyms (manually maintained or learned)
        self.value_synonyms = {
            "state": {
                "TG": ["telangana", "telengana", "tg", "t'gana"],
                "MH": ["maharashtra", "mh"],
                "KA": ["karnataka", "ka"]
            }
        }

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
    
    def _define_collection_relationships(self):
        """Define potential relationships between collections for joins"""
        # This should be customized based on your actual schema relationships
        # Example relationships - modify according to your actual data structure
        relationships = {
            ("report-agent-disposition", "report-history"): [
                {"field1": "agentId", "field2": "agentId"},
                {"field1": "callId", "field2": "callId"}
            ],
            # Add more relationships as needed
            # ("collection1", "collection2"): [{"field1": "commonField", "field2": "commonField"}]
        }
        return relationships
    
    def normalize_query(self, query):
        """Normalize natural language query using value synonyms"""
        normalized = query.lower()
        for field, synonym_map in self.value_synonyms.items():
            for canonical, variants in synonym_map.items():
                for variant in variants:
                    if variant.lower() in normalized:
                        normalized = normalized.replace(variant.lower(), canonical.lower())
        return normalized

    def _detect_query_type(self, query_text):
        """Determine if query needs single or multiple collections"""
        normalized_query = self.normalize_query(query_text)
        
        # Keywords that typically indicate multi-collection queries
        multi_collection_keywords = [
            "join", "combine", "merge", "correlate", "compare", "cross-reference",
            "relationship", "between", "and", "with", "against", "vs", "versus",
            "along with", "together with", "in relation to"
        ]
        
        # Count mentions of different collection types
        collection_mentions = 0
        for collection_name in self.schema_summaries.keys():
            collection_words = collection_name.lower().split('-')
            for word in collection_words:
                if word in normalized_query and len(word) > 3:  # Avoid short common words
                    collection_mentions += 1
                    break
        
        # Check for multi-collection keywords
        has_multi_keywords = any(keyword in normalized_query for keyword in multi_collection_keywords)
        
        # Determine query type
        if collection_mentions >= 2 or has_multi_keywords:
            return "multi_collection"
        else:
            return "single_collection"

    def _select_collections_for_multi_query(self, query_text):
        """Select multiple collections for a multi-collection query"""
        normalized_query = self.normalize_query(query_text)
        
        prompt = f"""
You are a MongoDB expert assistant. Given the following collection summaries, 
select the 2-3 most appropriate collections for this multi-collection query:

USER QUERY: "{normalized_query}"

AVAILABLE COLLECTIONS:
{self._format_collection_summaries()}

INSTRUCTIONS:
1. Analyze the user's query intent
2. Identify which collections need to be combined/joined
3. Select 2-3 collections that are most relevant
4. Consider potential relationships between collections
5. Respond with a JSON list of collection names

EXAMPLE RESPONSE:
["report-agent-disposition", "report-history"]

RESPONSE FORMAT: ["collection1", "collection2"]
"""
        response = self.model.generate_content(prompt)
        try:
            response_text = response.text.strip()
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            collections = json.loads(response_text)
            return collections if isinstance(collections, list) else [collections]
        except Exception as e:
            print(f"Error parsing collection selection: {str(e)}")
            return None

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
        response = self.model.generate_content(prompt)
        try:
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

    def _generate_multi_collection_query(self, query_text, collections):
        """Generate a multi-collection query strategy"""
        normalized_query = self.normalize_query(query_text)
        
        # Get schemas for selected collections
        schemas_text = "\n\n".join([
            self.full_schemas.get(coll, f"Schema not found for {coll}")
            for coll in collections
        ])
        
        # Get potential relationships
        relationships_text = self._format_relationships_for_collections(collections)
        
        # Prepare value synonyms string
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" 
                                    for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])
        
        prompt = f"""
You are a MongoDB expert specializing in multi-collection queries. Generate a query strategy for:

USER QUERY: "{normalized_query}"

SELECTED COLLECTIONS:
{schemas_text}

POTENTIAL RELATIONSHIPS:
{relationships_text}

VALUE SYNONYMS TO CONSIDER:
{value_synonyms_str}

Generate a JSON response with this structure:
{{
    "strategy": "sequential" | "lookup" | "separate_aggregate",
    "collections": {collections},
    "queries": [
        {{
            "collection": "collection_name",
            "query": <mongo_query>,
            "purpose": "description of what this query does"
        }}
    ],
    "join_logic": "description of how to combine results"
}}

STRATEGY OPTIONS:
1. "sequential" - Query collections one by one, use results from first to filter second
2. "lookup" - Use MongoDB $lookup for joins within aggregation pipeline
3. "separate_aggregate" - Run separate aggregations and combine results in application code

FOR LOOKUP STRATEGY - Generate a single aggregation pipeline with $lookup stages:
- Start with the primary collection
- Use $lookup to join with other collections
- Use $match stages for filtering
- Use $unwind if needed to flatten arrays from lookups

EXAMPLE LOOKUP QUERY:
{{
    "strategy": "lookup",
    "collections": ["report-agent-disposition", "report-history"],
    "queries": [
        {{
            "collection": "report-agent-disposition",
            "query": [
                {{ "$match": {{ "status": "completed" }} }},
                {{ "$lookup": {{
                    "from": "report-history",
                    "localField": "agentId",
                    "foreignField": "agentId",
                    "as": "agent_history"
                }} }},
                {{ "$unwind": {{ "path": "$agent_history", "preserveNullAndEmptyArrays": true }} }}
            ],
            "purpose": "Join agent dispositions with their history using $lookup"
        }}
    ],
    "join_logic": "MongoDB $lookup aggregation pipeline joins collections efficiently"
}}

CRITICAL RULES:
1. Use proper MongoDB query syntax
2. For text matching, use case-insensitive regex: {{ "$regex": "pattern", "$options": "i" }}
3. For LOOKUP strategy, create aggregation pipeline with $lookup stages
4. Consider data relationships when designing join strategy
5. Optimize for performance - avoid full collection scans when possible
6. Generate efficient queries with proper filtering

CHOOSE STRATEGY BASED ON:
- Use "lookup" when collections have clear relationships (foreign keys)
- Use "sequential" when you need results from first query to determine second query
- Use "separate_aggregate" when doing complex aggregations on each collection independently
"""
        
        response = self.model.generate_content(prompt)
        try:
            response_text = response.text
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            return json.loads(response_text.strip().strip('"\''))
        except Exception as e:
            return {"error": f"Failed to parse multi-collection query: {str(e)}", "raw_response": response.text}

    def _format_relationships_for_collections(self, collections):
        """Format relationship information for the given collections"""
        relationships = []
        for i, coll1 in enumerate(collections):
            for coll2 in collections[i+1:]:
                # Check both directions
                key1 = (coll1, coll2)
                key2 = (coll2, coll1)
                if key1 in self.collection_relationships:
                    relationships.append(f"{coll1} ↔ {coll2}: {self.collection_relationships[key1]}")
                elif key2 in self.collection_relationships:
                    relationships.append(f"{coll2} ↔ {coll1}: {self.collection_relationships[key2]}")
        
        return "\n".join(relationships) if relationships else "No predefined relationships found"
        
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
You are a MongoDB query expert. You think step by step before generating any query and you understand natural language queries very efficiently. Given this collection schema, generate a query for:

USER QUERY: "{normalized_query}"

COLLECTION SCHEMA:
{schema}

VALUE SYNONYMS TO CONSIDER (use canonical values in query):
{value_synonyms_str}

Respond ONLY with a JSON object: {{ "collection": "{collection_name}", "query": <mongo_query> }}

CRITICAL RULES:
1. Use proper MongoDB query syntax
2. For text matching, use case-insensitive regex: {{ "$regex": "pattern", "$options": "i" }}
-For EXACT text matching, use case-insensitive regex with ^ and $ anchors: {{ "$regex": "^pattern$", "$options": "i" }}
-For partial matching (if explicitly requested), use: {{ "$regex": "pattern", "$options": "i" }}
3. For counting, use aggregation pipeline with $group
4. NEVER use invalid top-level operators like $count, $sum
5. Match query terms to schema fields exactly
6. For state queries, consider both full names and abbreviations using $or
7. Generate efficient queries, don't generate slow queries
8. Generate queries about only what's required, don't generate queries about everything, use proper filtering

VALID QUERY FORMATS:
- Simple find: {{ "field": "value" }}
- Regex find: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
- Aggregation: [{{ "$match": {{ ... }} }}, {{ "$group": {{ ... }} }}]

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
- Simple exact match: {{ "field": "value" }}
- Exact regex match: {{ "field": {{ "$regex": "^value$", "$options": "i" }} }}
- Partial regex match: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
- Range find: {{ "field": {{ "$gte": 18, "$lte": 65 }} }}

EXAMPLES OF WHAT TO GENERATE:
- "find all users" → {{ "query": {{}} }}
- "count users" → {{ "query": [{{ "$group": {{ "_id": null, "count": {{ "$sum": 1 }} }} }}] }}
- "find users named john" → {{ "query": {{ "name": {{ "$regex": "john", "$options": "i" }} }} }}
- "users from telangana" → {{ "query": {{ "$or": [{{ "state": {{ "$regex": "telangana", "$options": "i" }} }}, {{ "state": {{ "$regex": "TG", "$options": "i" }} }}] }} }}

EXAMPLE OUTPUTS:
- {{ "collection": "report-agent-disposition", "query": {{ "callType": "inbound" }} }}
- {{ "collection": "report-history", "query": [{{ "$match": {{ "type": "agent-performance" }} }}] }}
"""
        response = self.model.generate_content(prompt)
        try:
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
        """Main method to convert NL to MongoDB query (handles both single and multi-collection)"""
        query_type = self._detect_query_type(natural_language_query)
        
        if query_type == "multi_collection":
            # Multi-collection query handling
            collections = self._select_collections_for_multi_query(natural_language_query)
            if not collections:
                return {"error": "Could not determine appropriate collections for multi-collection query"}
            
            query_result = self._generate_multi_collection_query(natural_language_query, collections)
            if "error" in query_result:
                return query_result
            
            # Store successful query in history
            self.query_history.append((natural_language_query, query_result))
            if len(self.query_history) > 10:
                self.query_history.pop(0)
            
            return query_result
        else:
            # Single collection query handling (existing logic)
            collection_name = self._select_best_collection(natural_language_query)
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

    def _execute_multi_collection_query(self, query_strategy):
        """Execute multi-collection query based on strategy"""
        strategy = query_strategy.get("strategy", "sequential")
        queries = query_strategy.get("queries", [])
        join_logic = query_strategy.get("join_logic", "")
        
        if strategy == "sequential":
            return self._execute_sequential_queries(queries, join_logic)
        elif strategy == "lookup":
            return self._execute_lookup_query(queries)
        elif strategy == "separate_aggregate":
            return self._execute_separate_aggregate(queries, join_logic)
        else:
            return {"error": f"Unsupported strategy: {strategy}"}

    def _execute_sequential_queries(self, queries, join_logic):
        """Execute queries sequentially, using results from previous queries"""
        all_results = []
        intermediate_data = {}
        
        try:
            for i, query_info in enumerate(queries):
                collection_name = query_info["collection"]
                query = query_info["query"]
                purpose = query_info.get("purpose", "")
                
                # If this is not the first query, potentially modify it based on previous results
                if i > 0 and "<agent_ids_from_first_query>" in str(query):
                    # Extract IDs from previous results for sequential filtering
                    if all_results:
                        prev_results = all_results[-1].get("results", [])
                        agent_ids = list(set([r.get("agentId") for r in prev_results if r.get("agentId")]))
                        # Replace placeholder with actual IDs
                        query_str = json.dumps(query).replace('"<agent_ids_from_first_query>"', json.dumps(agent_ids))
                        query = json.loads(query_str)
                
                # Execute the query
                result = self.execute_query(collection_name, query)
                result["collection"] = collection_name
                result["purpose"] = purpose
                all_results.append(result)
                
                # Store intermediate data for potential use
                if "results" in result:
                    intermediate_data[collection_name] = result["results"]
            
            # Combine results
            combined_results = self._combine_sequential_results(all_results, join_logic)
            return {
                "strategy": "sequential",
                "individual_results": all_results,
                "combined_results": combined_results,
                "join_logic": join_logic
            }
            
        except Exception as e:
            return {"error": f"Error executing sequential queries: {str(e)}"}

    def _execute_lookup_query(self, queries):
        """Execute a single aggregation query with $lookup joins"""
        try:
            if not queries:
                return {"error": "No queries provided for lookup strategy"}
            
            # For lookup strategy, we expect a single aggregation query with $lookup stages
            query_info = queries[0]
            collection_name = query_info["collection"]
            pipeline = query_info["query"]
            
            if not isinstance(pipeline, list):
                return {"error": "Lookup strategy requires aggregation pipeline"}
            
            # Validate that pipeline contains $lookup stages
            has_lookup = any("$lookup" in stage for stage in pipeline)
            if not has_lookup:
                return {"error": "Lookup strategy pipeline must contain $lookup stages"}
            
            # Execute the aggregation pipeline
            collection = self.db[collection_name]
            
            # Process pipeline for case-insensitive matching in $match stages
            processed_pipeline = []
            for stage in pipeline:
                if "$match" in stage:
                    stage["$match"] = self._convert_to_case_insensitive(stage["$match"])
                processed_pipeline.append(stage)
            
            # Execute aggregation with $lookup
            results = list(collection.aggregate(processed_pipeline, allowDiskUse=True))
            
            return {
                "strategy": "lookup",
                "results": json.loads(json.dumps(results, default=str)),
                "count": len(results),
                "query_type": "aggregate",
                "pipeline": processed_pipeline,
                "individual_results": [{
                    "collection": collection_name,
                    "count": len(results),
                    "purpose": query_info.get("purpose", "MongoDB $lookup aggregation"),
                    "results": results
                }],
                "combined_results": results  # For lookup, results are already combined
            }
            
        except Exception as e:
            return {"error": f"Error executing lookup query: {str(e)}"}

    def _generate_lookup_pipeline(self, primary_collection, secondary_collections, relationships, filters=None):
        """Generate MongoDB aggregation pipeline with $lookup stages"""
        pipeline = []
        
        # Add initial match stage if filters provided
        if filters:
            pipeline.append({"$match": filters})
        
        # Add $lookup stages for each secondary collection
        for secondary_collection in secondary_collections:
            # Find relationship between primary and secondary collection
            relationship = self._find_relationship(primary_collection, secondary_collection, relationships)
            
            if relationship:
                lookup_stage = {
                    "$lookup": {
                        "from": secondary_collection,
                        "localField": relationship["local_field"],
                        "foreignField": relationship["foreign_field"],
                        "as": f"{secondary_collection}_data"
                    }
                }
                pipeline.append(lookup_stage)
                
                # Optionally unwind if needed (can be configured)
                # pipeline.append({
                #     "$unwind": {
                #         "path": f"${secondary_collection}_data",
                #         "preserveNullAndEmptyArrays": True
                #     }
                # })
        
        return pipeline
    
    def _find_relationship(self, collection1, collection2, relationships):
        """Find relationship definition between two collections"""
        # Check both directions
        key1 = (collection1, collection2)
        key2 = (collection2, collection1)
        
        if key1 in self.collection_relationships:
            rel = self.collection_relationships[key1][0]  # Take first relationship
            return {"local_field": rel["field1"], "foreign_field": rel["field2"]}
        elif key2 in self.collection_relationships:
            rel = self.collection_relationships[key2][0]  # Take first relationship
            return {"local_field": rel["field2"], "foreign_field": rel["field1"]}
        
        # Default relationship assumption (common field names)
        common_fields = ["id", "_id", "agentId", "userId", "callId"]
        for field in common_fields:
            return {"local_field": field, "foreign_field": field}
        
        return None

    def _execute_separate_aggregate(self, queries, join_logic):
        """Execute separate aggregations and combine results"""
        all_results = []
        
        try:
            for query_info in queries:
                collection_name = query_info["collection"]
                query = query_info["query"]
                purpose = query_info.get("purpose", "")
                
                result = self.execute_query(collection_name, query)
                result["collection"] = collection_name
                result["purpose"] = purpose
                all_results.append(result)
            
            # Combine aggregated results
            combined_results = self._combine_aggregate_results(all_results, join_logic)
            return {
                "strategy": "separate_aggregate",
                "individual_results": all_results,
                "combined_results": combined_results,
                "join_logic": join_logic
            }
            
        except Exception as e:
            return {"error": f"Error executing separate aggregate queries: {str(e)}"}

    def _combine_sequential_results(self, results, join_logic):
        """Combine results from sequential queries"""
        if not results:
            return []
        
        # Simple combination - join on common fields or concatenate
        combined = []
        for result in results:
            if "results" in result:
                for item in result["results"]:
                    item["_source_collection"] = result["collection"]
                    combined.append(item)
        
        return combined

    def _combine_aggregate_results(self, results, join_logic):
        """Combine results from separate aggregations"""
        combined = {}
        total_count = 0
        
        for result in results:
            collection_name = result.get("collection", "unknown")
            if "results" in result:
                combined[collection_name] = result["results"]
                total_count += result.get("count", 0)
        
        combined["_total_count"] = total_count
        return combined
        
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
        
    def _generate_results_explanation(self, nl_query, query_result, results, is_multi_collection=False):
        """Generate user-friendly results explanation for both single and multi-collection queries"""
        
        if is_multi_collection:
            strategy = results.get("strategy", "unknown")
            individual_results = results.get("individual_results", [])
            combined_results = results.get("combined_results", [])
            
            total_count = sum(r.get("count", 0) for r in individual_results)
            
            prompt = f"""
You are a helpful MongoDB assistant explaining multi-collection query results in simple terms.

INSTRUCTIONS:
1. Say Hi 
2. Explain that this query involved multiple collections
3. Summarize the key findings from each collection
4. Highlight the relationships and patterns found across collections
5. Keep it concise but comprehensive (2-3 short paragraphs max)
6. Use natural, conversational language
7. Focus on insights from combining the data
8. Say the question the user asked in the beginning, in your own words

USER ORIGINAL QUESTION: "{nl_query}"

QUERY STRATEGY: {strategy}
TOTAL RESULTS ACROSS COLLECTIONS: {total_count}

INDIVIDUAL COLLECTION RESULTS:
{json.dumps([{"collection": r.get("collection"), "count": r.get("count"), "purpose": r.get("purpose")} for r in individual_results], indent=2)}

SAMPLE COMBINED RESULTS:
{json.dumps(combined_results if isinstance(combined_results, list) and len(combined_results) <= 3 else str(combined_results)[:500], indent=2, default=str)}
"""
        else:
            # Single collection explanation (existing logic)
            result_count = results.get('count', 0)
            sample_results = results.get('results', [])[:3]
            query_type = results.get('query_type', 'find')
            collection_name = query_result.get('collection', 'unknown')
            
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

USER ORIGINAL QUESTION: "{nl_query}"

DATABASE COLLECTION: {collection_name}
QUERY TYPE: {query_type}
NUMBER OF RESULTS: {result_count}

SAMPLE RESULTS (first 3):
{json.dumps(sample_results, indent=2, default=str)}
"""
        
        response = self.model.generate_content(prompt)
        return response.text

    def process_query(self, natural_language_query, include_explanation=True):
        """Complete end-to-end query processing for both single and multi-collection queries"""
        # Get query result (single or multi-collection)
        result = self.natural_language_to_query(natural_language_query)
        if "error" in result:
            return {
                "status": "error",
                "message": result["error"],
                "raw_response": result.get("raw_response", "")
            }

        # Check if this is a multi-collection query
        is_multi_collection = "strategy" in result
        
        if is_multi_collection:
            # Multi-collection query execution
            print(f'Multi-collection query using strategy: {result.get("strategy", "unknown")}')
            print(f'Collections: {result.get("collections", [])}')
            
            execution_result = self._execute_multi_collection_query(result)
            
            response = {
                "status": "success" if "error" not in execution_result else "error",
                "query_type": "multi_collection",
                "strategy": result.get("strategy"),
                "collections": result.get("collections", []),
                "generated_queries": result.get("queries", [])
            }
            
            if "error" in execution_result:
                response["message"] = execution_result["error"]
            else:
                response.update({
                    "results": execution_result,
                    "individual_results": execution_result.get("individual_results", []),
                    "combined_results": execution_result.get("combined_results", []),
                    "total_count": sum(r.get("count", 0) for r in execution_result.get("individual_results", []))
                })
                
                if include_explanation:
                    response["explanation"] = self._generate_results_explanation(
                        natural_language_query,
                        result,
                        execution_result,
                        is_multi_collection=True
                    )
        else:
            # Single collection query execution (existing logic)
            collection_name = result["collection"]
            print('The collection used by AI', collection_name)
            mongo_query = result["query"]

            # Execute query
            results = self.execute_query(collection_name, mongo_query)
            
            # Fallback logic for case sensitivity
            if "error" not in results and results.get("count", 0) == 0:
                ci_query = self._convert_to_case_insensitive(mongo_query)
                results = self.execute_query(collection_name, ci_query)
                mongo_query = ci_query

            # Build response
            response = {
                "status": "success" if "error" not in results else "error",
                "query_type": "single_collection",
                "collection": collection_name,
                "generated_query": mongo_query,
            }

            if "error" in results:
                response["message"] = results["error"]
            else:
                response.update({
                    "results": results["results"],
                    "count": results["count"],
                    "operation_type": results["query_type"]
                })
                if include_explanation:
                    response["explanation"] = self._generate_results_explanation(
                        natural_language_query,
                        result,
                        results,
                        is_multi_collection=False
                    )

        return response

    def get_query_history(self):
        """Return the query history"""
        return self.query_history
    
    def clear_query_history(self):
        """Clear the query history"""
        self.query_history = []
    
    def add_collection_relationship(self, collection1, collection2, relationships):
        """Add a new relationship between collections"""
        key = (collection1, collection2)
        self.collection_relationships[key] = relationships
        print(f"Added relationship between {collection1} and {collection2}: {relationships}")
    
    def get_supported_collections(self):
        """Return list of supported collections"""
        return list(self.schema_summaries.keys())
    
    def get_collection_schema(self, collection_name):
        """Get the full schema for a specific collection"""
        return self.full_schemas.get(collection_name, f"Schema not found for {collection_name}")

# Example usage and testing functions
# def test_multi_collection_system():
#     """Test function to demonstrate multi-collection capabilities"""
#     system = NLToMongoDBQuerySystem()
    
#     # Test queries

#     test_queries = [
#         # Single collection queries
#         '''Tell me how many people have their role as admin and how many processes have the type "both'''
        
#     ]
#     print("Testing Multi-Collection Query System")
#     print("=" * 50)
    
#     for i, query in enumerate(test_queries, 1):
#         print(f"\nTest {i}: {query}")
#         print("-" * 30)
        
#         result = system.process_query(query, include_explanation=True)
        
#         print(f"Status: {result['status']}")
#         print(f"Query Type: {result.get('query_type', 'unknown')}")
#         print(f"Generated Query: {result.get('generated_query', 'unknown')}")
        
#         if result['status'] == 'success':
#             if result.get('query_type') == 'multi_collection':
#                 print(f"Strategy: {result.get('strategy')}")
#                 print(f"Collections: {result.get('collections')}")
#                 print(f"Total Results: {result.get('total_count', 0)}")
#             else:
#                 print(f"Collection: {result.get('collection')}")
#                 print(f"Results Count: {result.get('count', 0)}")
            
#             if 'explanation' in result:
#                 print(f"Explanation: {result['explanation']}")
#         else:
#             print(f"Error: {result.get('message', 'Unknown error')}")
        
#         print()

# if __name__ == "__main__":
#     # Initialize and test the system
#     test_multi_collection_system()