def process_query(self, natural_language_query, include_explanation=True, analyze_performance=False, create_indexes_if_needed=None):
        """Complete end-to-end optimized query processing"""
        
        # Override auto_create_indexes if specified
        if create_indexes_if_needed is not None:
            original_auto_create = self.auto_create_indexes
            self.auto_create_indexes = create_indexes_if_needed
        
        # Phase 1 & 2: Get collection and optimized query
        result = self.natural_language_to_query(natural_language_query)
        
        # Restore original setting
        if create_indexes_if_needed is not None:
            self.auto_create_indexes = original_auto_create
            
        if "error" in result:
            return {
                "status": "error",
                "message": result["error"],
                "raw_response": result.get("raw_response", "")
            }

        collection_name = result["collection"]
        print('The collection used by AI:', collection_name)
        mongo_query = result["query"]
        
        # Execute query
        results = self.execute_query(collection_name, mongo_query)
        
        # Performance analysis if requested
        performance_info = None
        if analyze_performance:
            performance_info = self.analyze_query_performance(collection_name, mongo_query)
        
        # Fallback logic for case sensitivity
        if "error" not in results and results.get("count", 0) == 0:
            ci_query = self._convert_to_case_insensitive(mongo_query)
            results = self.execute_query(collection_name, ci_query)
            mongo_query = ci_query

        # Build response
        optimization_info = result.get("optimization_info", {})
        has_indexes = optimization_info.get("has_custom_indexes", False)
        
        response = {
            "status": "success" if "error" not in results else "error",
            "collection": collection_name,
            "generated_query": mongo_query,
            "optimization_info": optimization_info,
            "optimization_notes": result.get("optimization_notes", "No optimization information"),
            "has_indexes": has_indexes
        }
        
        # Add index recommendations if no indexes exist
        if not has_indexes:
            response["index_recommendations"] = optimization_info.get("index_recommendations")
            response["performance_warning"] = "This collection has no custom indexes. Consider creating indexes for better performance."
        
        if performance_info:
            response["performance_analysis"] = performance_info

        if "error" in results:
            response["message"] = results["error"]
        else:
            response.update({
                "results": results["results"],
                "count": results["count"],
                "query_type": results["query_type"],
                "execution_time": results.get("execution_time", 0)
            })
            if include_explanation:
                # Phase 3: Generate explanation with index context
                response["explanation"] = self._generate_results_explanation_with_index_context(
                    natural_language_query,
                    mongo_query,
                    results,
                    collection_name,
                    has_indexes
                )

        return response
    
def _generate_results_explanation_with_index_context(self, nl_query, mongo_query, results, collection_name, has_indexes):
        """Phase 3: Generate user-friendly results explanation with index performance context"""
        result_count = results.get('count', 0)
        sample_results = results.get('results', [])[:3]
        query_type = results.get('query_type', 'find')
        execution_time = results.get('execution_time', 0)
        
        # Performance context based on indexes
        performance_context = ""
        if not has_indexes and execution_time > 0.1:  # Slow query without indexes
            performance_context = "Note: This query took longer than expected because the collection doesn't have optimal indexes."
        elif has_indexes and execution_time < 0.05:  # Fast query with indexes
            performance_context = "This query was processed quickly using database indexes."
        
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
11. {f"Mention briefly that the query was optimized {('with indexes' if has_indexes else 'but could be faster with indexes')}" if execution_time > 0.05 else ""}

USER ORIGINAL QUESTION: "{nl_query}"

DATABASE COLLECTION: {collection_name}
QUERY TYPE: {query_type}
NUMBER OF RESULTS: {result_count}
EXECUTION TIME: {execution_time:.3f} seconds
HAS INDEXES: {has_indexes}
{performance_context}

QUERY USED:
{json.dumps(mongo_query, indent=2)}

SAMPLE RESULTS (first 3):
{json.dumps(sample_results, indent=2, default=str)}
"""
        response = self.model.generate_content(prompt)
        return response.text
    
def get_collection_status(self, collection_name=None):
        """Get comprehensive status of collections including index information"""
        if collection_name:
            collections = [collection_name]
        else:
            collections = list(self.collection_indexes.keys())
        
        status = {}
        for coll_name in collections:
            indexes_info = self.collection_indexes.get(coll_name, {})
            doc_count = self.db[coll_name].count_documents({})
            
            status[coll_name] = {
                "document_count": doc_count,
                "has_custom_indexes": indexes_info.get('has_custom_indexes', False),
                "single_field_indexes": indexes_info.get('single_field_indexes', []),
                "compound_indexes": indexes_info.get('compound_indexes', []),
                "recommended_indexes": indexes_info.get('recommended_single_indexes', []),
                "needs_indexes": indexes_info.get('needs_indexes', False),
                "performance_estimate": self._estimate_performance(doc_count, indexes_info.get('has_custom_indexes', False))
            }
        
        return status
    
def _estimate_performance(self, doc_count, has_indexes):
        """Estimate query performance based on document count and index availability"""
        if doc_count < 1000:
            return "Good - Small collection"
        elif doc_count < 10000:
            return "Good" if has_indexes else "Fair - Consider adding indexes"
        elif doc_count < 100000:
            return "Good" if has_indexes else "Poor - Indexes recommended"
        else:
            return "Good" if has_indexes else "Very Poor - Indexes critical"
    
def bulk_create_indexes(self, collections=None, force=False):
        """Create recommended indexes for multiple collections"""
        if not self.auto_create_indexes and not force:
            return {"message": "Auto-create indexes is disabled"}
        
        if collections is None:
            collections = [name for name, info in self.collection_indexes.items() 
                         if info.get('needs_indexes', False)]
        
        results = {}
        for collection_name in collections:
            result = self.create_recommended_indexes(collection_name, force=force)
            results[collection_name] = result
        
        return results
    
def explain_query_plan(self, collection_name, query):
        """Get detailed explanation of query execution plan"""
        try:
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            
            if operation_type == "find":
                filter_query = query.get("find", query)
                filter_query = self._convert_to_case_insensitive(filter_query)
                explain_result = collection.find(filter_query).explain("executionStats")
                
                # Extract key performance metrics
                execution_stats = explain_result.get("executionStats", {})
                winning_plan = explain_result.get("queryPlanner", {}).get("winningPlan", {})
                
                analysis = {
                    "query_successful": execution_stats.get("executionSuccess", False),
                    "total_docs_examined": execution_stats.get("totalDocsExamined", 0),
                    "total_docs_returned": execution_stats.get("totalDocsReturned", 0),
                    "execution_time_ms": execution_stats.get("executionTimeMillis", 0),
                    "index_used": winning_plan.get("stage") == "IXSCAN",
                    "stage": winning_plan.get("stage", "UNKNOWN"),
                    "index_name": winning_plan.get("indexName", "None"),
                    "performance_rating": self._rate_query_performance(execution_stats, winning_plan)
                }
                
                return analysis
                
        except Exception as e:
            return {"error": f"Could not explain query: {str(e)}"}
    
def _rate_query_performance(self, execution_stats, winning_plan):
        """Rate query performance based on execution statistics"""
        docs_examined = execution_stats.get("totalDocsExamined", 0)
        docs_returned = execution_stats.get("totalDocsReturned", 0)
        execution_time = execution_stats.get("executionTimeMillis", 0)
        uses_index = winning_plan.get("stage") == "IXSCAN"
        
        # Calculate efficiency ratio
        if docs_returned > 0:
            efficiency_ratio = docs_returned / max(docs_examined, 1)
        else:
            efficiency_ratio = 0
        
        if uses_index and efficiency_ratio > 0.1 and execution_time < 50:
            return "Excellent"
        elif uses_index and execution_time < 100:
            return "Good"
        elif uses_index:
            return "Fair"
        elif execution_time < 100:
            return "Poor - No index used"
        else:
            return "Very Poor - No index, slow execution"
        
import json
import google.generativeai as genai
from pymongo import MongoClient
import time
from difflib import get_close_matches


with open('backend/schema.txt', 'r') as file:
    file_contents = file.read()

SCHEMAS_STR = file_contents


class NLToMongoDBQuerySystem:
    def __init__(self, auto_create_indexes=True):
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
        self.auto_create_indexes = auto_create_indexes
        
        # Schema processing
        self.schema_summaries = self._create_schema_summaries()
        self.full_schemas = self._parse_full_schemas()
        
        # Index information for each collection
        self.collection_indexes = self._get_collection_indexes()
        
        # Track query patterns for index recommendations
        self.query_patterns = {}
        self.index_recommendations = {}

        # Value synonyms (manually maintained or learned)
        self.value_synonyms = {
            "state": {
                "TG": ["telangana", "telengana", "tg", "t'gana"],
                "MH": ["maharashtra", "mh"],
                "KA": ["karnataka", "ka"]
            }
        }

    def _get_collection_indexes(self):
        """Retrieve index information for all collections"""
        indexes = {}
        try:
            for collection_name in self.db.list_collection_names():
                collection = self.db[collection_name]
                index_info = list(collection.list_indexes())
                
                # Extract indexed fields
                indexed_fields = []
                compound_indexes = []
                
                for index in index_info:
                    if index['name'] != '_id_':  # Skip default _id index
                        index_keys = list(index['key'].keys())
                        if len(index_keys) == 1:
                            indexed_fields.append(index_keys[0])
                        else:
                            compound_indexes.append(index_keys)
                
                indexes[collection_name] = {
                    'single_field_indexes': indexed_fields,
                    'compound_indexes': compound_indexes,
                    'all_indexed_fields': list(set(field for idx in compound_indexes for field in idx) | set(indexed_fields)),
                    'has_custom_indexes': len(indexed_fields) > 0 or len(compound_indexes) > 0
                }
                
                # If no custom indexes, recommend based on schema
                if not indexes[collection_name]['has_custom_indexes']:
                    recommended = self._recommend_indexes_from_schema(collection_name)
                    indexes[collection_name].update(recommended)
                    
        except Exception as e:
            print(f"Warning: Could not retrieve index information: {e}")
            # Fallback: Common indexed fields based on typical patterns
            indexes = self._get_fallback_indexes()
        
        return indexes
    
    def _recommend_indexes_from_schema(self, collection_name):
        """Recommend indexes based on schema analysis when no indexes exist"""
        schema_text = self.full_schemas.get(collection_name, "")
        
        # Common patterns for fields that should be indexed
        index_worthy_patterns = [
            'id', 'Id', 'ID', '_id',  # ID fields
            'email', 'username', 'login',  # User identifiers
            'timestamp', 'createdAt', 'updatedAt', 'date', 'time',  # Time-based fields
            'status', 'state', 'type', 'category',  # Status/category fields
            'userId', 'agentId', 'customerId',  # Foreign keys
            'priority', 'level', 'rank',  # Priority fields
            'active', 'enabled', 'deleted',  # Boolean flags
        ]
        
        recommended_single = []
        recommended_compound = []
        
        # Extract field names from schema
        lines = schema_text.split('\n')
        schema_fields = []
        for line in lines:
            if ':' in line and not line.strip().startswith('//'):
                field_name = line.split(':')[0].strip().strip('"\'')
                schema_fields.append(field_name)
        
        # Recommend single field indexes
        for field in schema_fields:
            for pattern in index_worthy_patterns:
                if pattern.lower() in field.lower():
                    recommended_single.append(field)
                    break
        
        # Recommend compound indexes based on common patterns
        if any('timestamp' in f.lower() or 'date' in f.lower() or 'time' in f.lower() for f in schema_fields):
            time_fields = [f for f in schema_fields if any(t in f.lower() for t in ['timestamp', 'date', 'time', 'created', 'updated'])]
            id_fields = [f for f in schema_fields if any(t in f.lower() for t in ['id', 'user', 'agent', 'customer']) and 'id' in f.lower()]
            
            for id_field in id_fields[:2]:  # Limit to avoid too many indexes
                for time_field in time_fields[:1]:  # Most recent time field
                    if id_field != time_field:
                        recommended_compound.append([id_field, time_field])
        
        return {
            'recommended_single_indexes': recommended_single[:5],  # Limit recommendations
            'recommended_compound_indexes': recommended_compound[:3],
            'needs_indexes': True
        }
    
    def create_recommended_indexes(self, collection_name, force=False):
        """Create recommended indexes for a collection"""
        if not self.auto_create_indexes and not force:
            return {"message": "Auto-create indexes is disabled"}
            
        indexes_info = self.collection_indexes.get(collection_name, {})
        
        if not indexes_info.get('needs_indexes', False):
            return {"message": "Collection already has indexes or no recommendations available"}
        
        collection = self.db[collection_name]
        created_indexes = []
        errors = []
        
        try:
            # Create single field indexes
            for field in indexes_info.get('recommended_single_indexes', []):
                try:
                    index_name = f"{field}_1"
                    collection.create_index([(field, 1)], name=index_name, background=True)
                    created_indexes.append(f"Single field index on '{field}'")
                except Exception as e:
                    errors.append(f"Failed to create index on {field}: {str(e)}")
            
            # Create compound indexes
            for fields in indexes_info.get('recommended_compound_indexes', []):
                try:
                    index_spec = [(field, 1) for field in fields]
                    index_name = f"{'_'.join(fields)}_compound"
                    collection.create_index(index_spec, name=index_name, background=True)
                    created_indexes.append(f"Compound index on {fields}")
                except Exception as e:
                    errors.append(f"Failed to create compound index on {fields}: {str(e)}")
            
            # Update our index information
            if created_indexes:
                self.collection_indexes = self._get_collection_indexes()
                
            return {
                "created_indexes": created_indexes,
                "errors": errors,
                "total_created": len(created_indexes)
            }
            
        except Exception as e:
            return {"error": f"Failed to create indexes: {str(e)}"}
    
    def get_index_recommendations(self, collection_name):
        """Get index recommendations for a collection"""
        indexes_info = self.collection_indexes.get(collection_name, {})
        
        if indexes_info.get('has_custom_indexes', False):
            return {"message": "Collection already has custom indexes"}
        
        recommendations = {
            "collection": collection_name,
            "current_indexes": indexes_info.get('single_field_indexes', []),
            "recommended_single_indexes": indexes_info.get('recommended_single_indexes', []),
            "recommended_compound_indexes": indexes_info.get('recommended_compound_indexes', []),
            "benefits": self._explain_index_benefits(indexes_info)
        }
        
        return recommendations
    
    def _explain_index_benefits(self, indexes_info):
        """Explain the benefits of recommended indexes"""
        benefits = []
        
        single_indexes = indexes_info.get('recommended_single_indexes', [])
        compound_indexes = indexes_info.get('recommended_compound_indexes', [])
        
        for field in single_indexes:
            if 'id' in field.lower():
                benefits.append(f"Index on '{field}' will speed up lookups and joins")
            elif any(t in field.lower() for t in ['timestamp', 'date', 'time', 'created']):
                benefits.append(f"Index on '{field}' will speed up time-based queries and sorting")
            elif any(t in field.lower() for t in ['status', 'type', 'category', 'state']):
                benefits.append(f"Index on '{field}' will speed up filtering by categories")
        
        for fields in compound_indexes:
            benefits.append(f"Compound index on {fields} will optimize queries filtering by multiple criteria")
        
        return benefits
    
    def _get_fallback_indexes(self):
        """Fallback index information for common patterns"""
        return {
            # Add your known indexes here
            "report-agent-disposition": {
                'single_field_indexes': ['agentId', 'callType', 'timestamp', 'disposition'],
                'compound_indexes': [['agentId', 'timestamp'], ['callType', 'disposition']],
                'all_indexed_fields': ['agentId', 'callType', 'timestamp', 'disposition']
            },
            "report-history": {
                'single_field_indexes': ['userId', 'type', 'createdAt', 'status'],
                'compound_indexes': [['userId', 'createdAt'], ['type', 'status']],
                'all_indexed_fields': ['userId', 'type', 'createdAt', 'status']
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
    
    def normalize_query(self, query):
        """Normalize natural language query using value synonyms"""
        normalized = query.lower()
        for field, synonym_map in self.value_synonyms.items():
            for canonical, variants in synonym_map.items():
                for variant in variants:
                    if variant.lower() in normalized:
                        normalized = normalized.replace(variant.lower(), canonical.lower())
        return normalized

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
    
    def _format_index_information(self, collection_name):
        """Format index information for the query generation prompt"""
        indexes = self.collection_indexes.get(collection_name, {})
        if not indexes:
            return "No index information available."
        
        info = []
        if indexes.get('single_field_indexes'):
            info.append(f"SINGLE FIELD INDEXES: {', '.join(indexes['single_field_indexes'])}")
        
        if indexes.get('compound_indexes'):
            compound_str = []
            for compound in indexes['compound_indexes']:
                compound_str.append(f"({', '.join(compound)})")
            info.append(f"COMPOUND INDEXES: {', '.join(compound_str)}")
        
        return "\n".join(info)
        
    def _generate_query_for_collection(self, query_text, collection_name):
        """Phase 2: Generate optimized query for specific collection using indexes"""
        normalized_query = self.normalize_query(query_text)
        schema = self.full_schemas.get(collection_name)
        if not schema:
            return {"error": f"Collection {collection_name} not found"}
        
        # Get index information for this collection
        indexes_info = self.collection_indexes.get(collection_name, {})
        has_indexes = indexes_info.get('has_custom_indexes', False)
        
        # Handle no indexes scenario
        if not has_indexes:
            # Check if we should create indexes
            if self.auto_create_indexes and indexes_info.get('needs_indexes', False):
                print(f"No indexes found for {collection_name}. Creating recommended indexes...")
                index_creation_result = self.create_recommended_indexes(collection_name)
                print(f"Index creation result: {index_creation_result}")
                
                # Refresh index information
                self.collection_indexes = self._get_collection_indexes()
                indexes_info = self.collection_indexes.get(collection_name, {})
        
        index_info = self._format_index_information(collection_name)
        indexed_fields = indexes_info.get('all_indexed_fields', [])
        recommended_fields = indexes_info.get('recommended_single_indexes', [])
        
        # Prepare value synonyms string
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" 
                                    for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])
        
        print(f"Schema: {schema}")
        print(f"Index info: {index_info}")
        print(f"Has custom indexes: {has_indexes}")
            
        # Adjust prompt based on index availability
        if has_indexes:
            optimization_instructions = f"""
OPTIMIZATION PRIORITY (CRITICAL - INDEXES AVAILABLE):
1. **ALWAYS USE INDEXED FIELDS FIRST** - Structure queries to utilize indexed fields: {', '.join(indexed_fields)}
2. **COMPOUND INDEX OPTIMIZATION** - When using compound indexes, follow the index field order
3. **RANGE QUERIES** - Use indexed fields for range queries ($gte, $lte, $gt, $lt)
4. **SORTING** - Only sort on indexed fields to avoid in-memory sorts
5. **LIMIT EARLY** - Apply filters on indexed fields before other operations
6. **AGGREGATION OPTIMIZATION** - Place $match stages with indexed fields first in pipeline

AVAILABLE INDEXED FIELDS: {', '.join(indexed_fields)}
"""
        else:
            optimization_instructions = f"""
OPTIMIZATION PRIORITY (NO INDEXES DETECTED):
1. **QUERY SELECTIVITY** - Structure queries to filter data as early as possible
2. **AVOID FULL COLLECTION SCANS** - Use specific field matches rather than broad searches
3. **RECOMMENDED INDEXABLE FIELDS** - Consider these fields for best performance: {', '.join(recommended_fields)}
4. **LIMIT RESULTS** - Always limit results when possible to improve performance
5. **SIMPLE OPERATIONS** - Prefer simple find operations over complex aggregations when possible

WARNING: This collection has no custom indexes. Queries may be slow on large datasets.
RECOMMENDED FIELDS TO INDEX: {', '.join(recommended_fields)}
"""
            
        prompt = f"""
You are a MongoDB query optimization expert. Generate the MOST EFFICIENT query possible.

USER QUERY: "{normalized_query}"

COLLECTION SCHEMA:
{schema}

INDEX INFORMATION:
{index_info}

{optimization_instructions}

VALUE SYNONYMS TO CONSIDER (use canonical values in query):
{value_synonyms_str}

QUERY STRUCTURE RULES:
- For simple queries: {{ "field": "value", "other_field": "value2" }}
- For aggregations: [{{ "$match": {{ "field": criteria }} }}, {{ "$group": ... }}]
- {"Always put indexed field conditions first in $match stages" if has_indexes else "Structure queries for maximum selectivity"}
- Use $sort only on {"indexed fields when possible" if has_indexes else "fields that will be most selective"}
- For text search, use {"indexed text fields with regex" if has_indexes else "specific field matches with regex"}

CRITICAL MONGODB SYNTAX RULES:
1. Use proper MongoDB query syntax
2. For text matching, use case-insensitive regex: {{ "$regex": "pattern", "$options": "i" }}
3. For EXACT text matching: {{ "$regex": "^pattern$", "$options": "i" }}
4. For counting, use aggregation pipeline with $group
5. NEVER use invalid top-level operators like $count, $sum
6. Match query terms to schema fields exactly
7. For state queries, consider both full names and abbreviations using $or

PERFORMANCE OPTIMIZATION:
- {"Structure queries to use indexes efficiently" if has_indexes else "Structure queries to minimize data scanning"}
- {"Avoid queries that can't use indexes (full collection scans)" if has_indexes else "Use the most selective filters first"}
- For aggregations, put {"indexed field filters" if has_indexes else "most selective filters"} in early $match stages
- {"Use compound indexes in the correct field order" if has_indexes else "Consider which fields would benefit from indexing"}
- Limit results when possible to improve performance

VALID QUERY FORMATS ONLY:
- Optimized find: {{ "field": "value", "field2": "value2" }}
- Optimized regex: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
- Optimized range: {{ "field": {{ "$gte": start, "$lte": end }} }}
- Optimized aggregation: [{{ "$match": {{ "field": criteria }} }}, {{ "$group": {{ "_id": "$field", "count": {{ "$sum": 1 }} }} }}]

Respond ONLY with a JSON object: {{ "collection": "{collection_name}", "query": <optimized_mongo_query>, "optimization_notes": "explanation of optimization approach{"and index usage" if has_indexes else " (no indexes available)"}" }}
"""
        
        response = self.model.generate_content(prompt)
        try:
            # Parse response
            response_text = response.text
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(response_text.strip().strip('"\''))
            
            # Add optimization metadata
            result["optimization_info"] = {
                "has_custom_indexes": has_indexes,
                "available_indexes": indexed_fields,
                "recommended_indexes": recommended_fields,
                "compound_indexes": indexes_info.get('compound_indexes', []),
                "index_recommendations": self.get_index_recommendations(collection_name) if not has_indexes else None
            }
            
            return result
        except Exception as e:
            return {"error": f"Failed to parse query: {str(e)}", "raw_response": response.text}

    def natural_language_to_query(self, natural_language_query):
        """Main method to convert NL to optimized MongoDB query"""
        # Phase 1: Collection selection
        collection_name = self._select_best_collection(natural_language_query)
        if not collection_name:
            return {"error": "Could not determine appropriate collection"}
        
        # Phase 2: Optimized query generation
        query_result = self._generate_query_for_collection(natural_language_query, collection_name)
        if "error" in query_result:
            return query_result
            
        # Store successful query in history
        self.query_history.append((natural_language_query, query_result))
        if len(self.query_history) > 10:
            self.query_history.pop(0)
            
        return query_result
        
    def _convert_to_case_insensitive(self, query):
        """Convert query to case-insensitive version while preserving index usage"""
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
        """Execute the generated MongoDB query with performance monitoring"""
        try:
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            results = None
            
            # Start timing for performance monitoring
            start_time = time.time()
            
            if operation_type == "aggregate":
                pipeline = query if isinstance(query, list) else query.get("aggregate", [])
                # Process each $match stage for case-insensitive matching
                processed_pipeline = []
                for stage in pipeline:
                    if "$match" in stage:
                        stage["$match"] = self._convert_to_case_insensitive(stage["$match"])
                    processed_pipeline.append(stage)
                
                # Add explain option for performance analysis in development
                results = list(collection.aggregate(processed_pipeline, allowDiskUse=True))
                
            elif operation_type == "find":
                filter_query = query.get("find", query)
                filter_query = self._convert_to_case_insensitive(filter_query)
                cursor = collection.find(filter_query)
                results = list(cursor)
            else:
                return {"error": f"Unsupported operation type: {operation_type}"}
            
            execution_time = time.time() - start_time
            
            return {
                "results": json.loads(json.dumps(results, default=str)),
                "count": len(results),
                "query_type": operation_type,
                "execution_time": execution_time
            }
        except Exception as e:
            return {"error": str(e)}
    
    def analyze_query_performance(self, collection_name, query):
        """Analyze query performance using MongoDB explain"""
        try:
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            
            if operation_type == "find":
                filter_query = query.get("find", query)
                filter_query = self._convert_to_case_insensitive(filter_query)
                explain_result = collection.find(filter_query).explain()
                
                return {
                    "index_used": explain_result.get("executionStats", {}).get("executionSuccess", False),
                    "execution_stats": explain_result.get("executionStats", {}),
                    "winning_plan": explain_result.get("queryPlanner", {}).get("winningPlan", {})
                }
            elif operation_type == "aggregate":
                # For aggregation, we can use explain with the pipeline
                pipeline = query if isinstance(query, list) else query.get("aggregate", [])
                explain_result = collection.aggregate(pipeline, explain=True)
                return {"explain": list(explain_result)}
                
        except Exception as e:
            return {"error": f"Could not analyze query performance: {str(e)}"}
        
    def _generate_results_explanation(self, nl_query, mongo_query, results, collection_name):
        """Phase 3: Generate user-friendly results explanation with performance info"""
        result_count = results.get('count', 0)
        sample_results = results.get('results', [])[:3]
        query_type = results.get('query_type', 'find')
        execution_time = results.get('execution_time', 0)
        
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
11. If the query was fast (< 100ms), you can mention it was quick to process

USER ORIGINAL QUESTION: "{nl_query}"

DATABASE COLLECTION: {collection_name}
QUERY TYPE: {query_type}
NUMBER OF RESULTS: {result_count}
EXECUTION TIME: {execution_time:.3f} seconds

QUERY USED:
{json.dumps(mongo_query, indent=2)}

SAMPLE RESULTS (first 3):
{json.dumps(sample_results, indent=2, default=str)}
"""
        response = self.model.generate_content(prompt)
        return response.text

    def process_query(self, natural_language_query, include_explanation=True, analyze_performance=False):
        """Complete end-to-end optimized query processing"""
        # Phase 1 & 2: Get collection and optimized query
        result = self.natural_language_to_query(natural_language_query)
        if "error" in result:
            return {
                "status": "error",
                "message": result["error"],
                "raw_response": result.get("raw_response", "")
            }

        collection_name = result["collection"]
        print('The collection used by AI:', collection_name)
        mongo_query = result["query"]
        
        # Execute query
        results = self.execute_query(collection_name, mongo_query)
        
        # Performance analysis if requested
        performance_info = None
        if analyze_performance:
            performance_info = self.analyze_query_performance(collection_name, mongo_query)
        
        # Fallback logic for case sensitivity
        if "error" not in results and results.get("count", 0) == 0:
            ci_query = self._convert_to_case_insensitive(mongo_query)
            results = self.execute_query(collection_name, ci_query)
            mongo_query = ci_query

        # Build response
        response = {
            "status": "success" if "error" not in results else "error",
            "collection": collection_name,
            "generated_query": mongo_query,
            "optimization_info": result.get("optimization_info", {}),
            "index_usage_explanation": result.get("index_usage", "No index usage information")
        }
        
        if performance_info:
            response["performance_analysis"] = performance_info

        if "error" in results:
            response["message"] = results["error"]
        else:
            response.update({
                "results": results["results"],
                "count": results["count"],
                "query_type": results["query_type"],
                "execution_time": results.get("execution_time", 0)
            })
            if include_explanation:
                # Phase 3: Generate explanation
                response["explanation"] = self._generate_results_explanation(
                    natural_language_query,
                    mongo_query,
                    results,
                    collection_name
                )

        return response