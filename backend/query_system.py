import json
import google.generativeai as genai
from pymongo import MongoClient
import os
import time
from difflib import get_close_matches
from bson import ObjectId
from typing import Optional, Dict, Any, List


class CompanyAccessManager:
    """Handles company-based access control and data filtering"""
    
    def __init__(self, db):
        self.db = db
        
        # Collections that require company-level filtering
        self.company_scoped_collections = {
            "license", "email-interaction", "rechurn-log", "report-history",
            "api-key", "call-interaction", "customer-assign-log", "transaction",
            "roles", "crm-interaction", "allocation", "time-log", "user",
            "customer", "whatsapp-interaction", "process", "report-agent-login",
            "sms-interaction", "email-template", "whatsapp-template",
            "cloud-virtual-number", "customer-details", "report-agent-disposition",
            "crm-field", "rechurn-status", "recurring-interaction", "sms-template"
        }
        
        # Collections that don't require company filtering (system-wide)
        self.global_collections = {
            "system-config", "global-settings", "audit-log"
        }
    
    def validate_company_access(self, company_id: str) -> bool:
        """Validate if the company ID exists and is active"""
        try:
            company_obj_id = ObjectId(company_id)
            company = self.db.company.find_one({
                "_id": company_obj_id,
                "status": {"$ne": "deleted"}  # Exclude deleted companies
            })
            return company is not None
        except Exception as e:
            print(f"[ERROR] Company validation failed: {e}")
            return False
    
    def is_company_scoped(self, collection_name: str) -> bool:
        """Check if a collection requires company-level filtering"""
        return collection_name in self.company_scoped_collections
    
    def inject_company_filter(self, query: Any, company_id: str, collection_name: str) -> Any:
        """Inject company ID filter into query based on collection type"""
        if not self.is_company_scoped(collection_name):
            return query
            
        company_obj_id = ObjectId(company_id)
        
        if isinstance(query, list):  # Aggregation pipeline
            # Add company filter as the first $match stage
            company_match = {"$match": {"Cid": company_obj_id}}
            
            # Check if first stage is already a $match, if so merge the filters
            if query and "$match" in query[0]:
                query[0]["$match"]["Cid"] = company_obj_id
                return query
            else:
                return [company_match] + query
                
        else:  # Find operation
            if isinstance(query, dict):
                # Add company filter to existing query
                if "Cid" not in query:
                    if "$and" in query:
                        query["$and"].append({"Cid": company_obj_id})
                    else:
                        query = {"$and": [query, {"Cid": company_obj_id}]}
                return query
            else:
                # Simple query, wrap with company filter
                return {"$and": [query, {"Cid": company_obj_id}]}
        
        return query


class NLToMongoDBQuerySystem:
    def __init__(self, company_id: str, auto_create_indexes: bool = True):
        """
        Initialize the NL to MongoDB Query System with company-based access control
        
        Args:
            company_id: The company ID for data access control
            auto_create_indexes: Whether to automatically create recommended indexes
        """
        # Configuration - replace with your actual values
        self.API_KEY = "AIzaSyDzq0RE9mmQR6ipTNu4AffCGU6u7FmXQ38"
        self.MONGODB_URI = "mongodb://localhost:27017"
        self.DB_NAME = "runo"
        
        # Company access control
        self.company_id = company_id
        
        # Initialize MongoDB connection
        self.client = MongoClient(self.MONGODB_URI)
        self.db = self.client[self.DB_NAME]
        
        # Initialize access manager
        self.access_manager = CompanyAccessManager(self.db)
        
        # Validate company access
        if not self.access_manager.validate_company_access(company_id):
            raise ValueError(f"Invalid or inactive company ID: {company_id}")
        
        # Initialize Gemini
        genai.configure(api_key=self.API_KEY)
        self.model = genai.GenerativeModel('gemini-2.5-flash')

        # System state
        self.query_history = []
        self.auto_create_indexes = auto_create_indexes
        
        # Load schema information
        self.schema_content = self._load_schema()
        
        # Schema processing
        self.schema_summaries = self._create_schema_summaries()
        self.full_schemas = self._parse_full_schemas()
        
        # Index information for each collection
        self.collection_indexes = self._get_collection_indexes()
        
        # Track query patterns for index recommendations
        self.query_patterns = {}
        self.index_recommendations = {}

        # Value synonyms for better query understanding
        self.value_synonyms = {
            "state": {
                "TG": ["telangana", "telengana", "tg", "t'gana"],
                "MH": ["maharashtra", "mh"],
                "KA": ["karnataka", "ka"]
            }
        }

    def _load_schema(self) -> str:
        """Load schema from file with error handling"""
        try:
            with open('backend/schema.txt', 'r') as file:
                return file.read()
        except FileNotFoundError:
            print("[WARNING] Schema file not found. Using empty schema.")
            return ""
        except Exception as e:
            print(f"[ERROR] Failed to load schema: {e}")
            return ""

    def _rate_query_performance(self, execution_stats: Dict, winning_plan: Dict) -> str:
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

    def _generate_results_explanation_with_index_context(self, nl_query: str, mongo_query: Any, 
                                                       results: Dict, collection_name: str, 
                                                       has_indexes: bool) -> str:
        """Generate user-friendly results explanation with index performance context"""
        result_count = results.get('count', 0)
        sample_results = results.get('results', [])[:3]
        query_type = results.get('query_type', 'find')
        execution_time = results.get('execution_time', 0)
        
        # Performance context based on indexes
        performance_context = ""
        if not has_indexes and execution_time > 0.1:
            performance_context = "Note: This query took longer than expected because the collection doesn't have optimal indexes."
        elif has_indexes and execution_time < 0.05:
            performance_context = "This query was processed quickly using database indexes."
        
        prompt = f"""
You are a helpful MongoDB assistant explaining query results in simple terms.

INSTRUCTIONS:
1. Say Hi 
2. Summarize the key findings from the results
3. Highlight any important numbers or patterns
4. Keep it concise (1-2 short paragraphs max)
5. Use natural, conversational language, but also keep it professional and simple
6. If no results found, suggest possible reasons
7. Tell it in a way that seems natural and human-like, not robotic
8. Avoid technical jargon, use simple terms, and keep it friendly
9. Don't talk about the query used or how it was generated, focus on the results
10. Say the question the user asked in the beginning, in your own words
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

    def _get_collection_indexes(self) -> Dict:
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
            print(f"[WARNING] Could not retrieve index information: {e}")
            indexes = self._get_fallback_indexes()
        
        return indexes

    def _recommend_indexes_from_schema(self, collection_name: str) -> Dict:
        """Recommend indexes based on schema analysis when no indexes exist"""
        schema_text = self.full_schemas.get(collection_name, "")
        
        # Common patterns for fields that should be indexed
        index_worthy_patterns = [
            'id', 'Id', 'ID', '_id', 'Cid',  # ID fields (including company ID)
            'email', 'username', 'login',  # User identifiers
            'timestamp', 'createdAt', 'updatedAt', 'date', 'time',  # Time-based fields
            'status', 'state', 'type', 'category',  # Status/category fields
            'userId', 'agentId', 'customerId',  # Foreign keys
            'priority', 'level', 'rank',  # Priority fields
            'active', 'enabled', 'deleted',  # Boolean flags
        ]
        
        recommended_single = []
        recommended_compound = []
        
        # Always recommend Cid index for company-scoped collections
        if self.access_manager.is_company_scoped(collection_name):
            recommended_single.append('Cid')
        
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
                if pattern.lower() in field.lower() and field not in recommended_single:
                    recommended_single.append(field)
                    break
        
        # Recommend compound indexes based on common patterns
        if any('timestamp' in f.lower() or 'date' in f.lower() or 'time' in f.lower() for f in schema_fields):
            time_fields = [f for f in schema_fields if any(t in f.lower() for t in ['timestamp', 'date', 'time', 'created', 'updated'])]
            id_fields = [f for f in schema_fields if any(t in f.lower() for t in ['id', 'user', 'agent', 'customer']) and 'id' in f.lower()]
            
            # Add Cid to compound indexes for company-scoped collections
            if self.access_manager.is_company_scoped(collection_name):
                for time_field in time_fields[:1]:  # Most recent time field
                    recommended_compound.append(['Cid', time_field])
                for id_field in id_fields[:1]:  # First ID field
                    if id_field != 'Cid':
                        recommended_compound.append(['Cid', id_field])
            
            # Regular compound indexes
            for id_field in id_fields[:2]:
                for time_field in time_fields[:1]:
                    if id_field != time_field and [id_field, time_field] not in recommended_compound:
                        recommended_compound.append([id_field, time_field])
        
        return {
            'recommended_single_indexes': recommended_single[:5],  # Limit recommendations
            'recommended_compound_indexes': recommended_compound[:3],
            'needs_indexes': True
        }

    def create_recommended_indexes(self, collection_name: str, force: bool = False) -> Dict:
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

    def get_index_recommendations(self, collection_name: str) -> Dict:
        """Get index recommendations for a collection"""
        indexes_info = self.collection_indexes.get(collection_name, {})
        
        if indexes_info.get('has_custom_indexes', False):
            return {"message": "Collection already has custom indexes"}
        
        recommendations = {
            "collection": collection_name,
            "current_indexes": indexes_info.get('single_field_indexes', []),
            "recommended_single_indexes": indexes_info.get('recommended_single_indexes', []),
            "recommended_compound_indexes": indexes_info.get('recommended_compound_indexes', []),
            "benefits": self._explain_index_benefits(indexes_info),
            "company_scoped": self.access_manager.is_company_scoped(collection_name)
        }
        
        return recommendations

    def _explain_index_benefits(self, indexes_info: Dict) -> List[str]:
        """Explain the benefits of recommended indexes"""
        benefits = []
        
        single_indexes = indexes_info.get('recommended_single_indexes', [])
        compound_indexes = indexes_info.get('recommended_compound_indexes', [])
        
        for field in single_indexes:
            if field == 'Cid':
                benefits.append(f"Index on '{field}' is essential for company-level data isolation and performance")
            elif 'id' in field.lower():
                benefits.append(f"Index on '{field}' will speed up lookups and joins")
            elif any(t in field.lower() for t in ['timestamp', 'date', 'time', 'created']):
                benefits.append(f"Index on '{field}' will speed up time-based queries and sorting")
            elif any(t in field.lower() for t in ['status', 'type', 'category', 'state']):
                benefits.append(f"Index on '{field}' will speed up filtering by categories")
        
        for fields in compound_indexes:
            if 'Cid' in fields:
                benefits.append(f"Compound index on {fields} will optimize company-scoped queries with multiple criteria")
            else:
                benefits.append(f"Compound index on {fields} will optimize queries filtering by multiple criteria")
        
        return benefits

    def _get_fallback_indexes(self) -> Dict:
        """Fallback index information for common patterns"""
        return {
            "report-agent-disposition": {
                'single_field_indexes': ['Cid', 'agentId', 'callType', 'timestamp', 'disposition'],
                'compound_indexes': [['Cid', 'agentId'], ['Cid', 'timestamp'], ['agentId', 'timestamp']],
                'all_indexed_fields': ['Cid', 'agentId', 'callType', 'timestamp', 'disposition'],
                'has_custom_indexes': True
            },
            "report-history": {
                'single_field_indexes': ['Cid', 'userId', 'type', 'createdAt', 'status'],
                'compound_indexes': [['Cid', 'userId'], ['Cid', 'createdAt'], ['userId', 'createdAt']],
                'all_indexed_fields': ['Cid', 'userId', 'type', 'createdAt', 'status'],
                'has_custom_indexes': True
            }
        }

    def _create_schema_summaries(self) -> Dict[str, str]:
        """Create concise one-line summaries of each collection"""
        summaries = {}
        if not self.schema_content:
            return summaries
            
        collections = self.schema_content.strip().split("Collection: ")[1:]
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            first_line = coll_text[name_end+1:].split('\n')[0].strip()
            summaries[collection_name] = first_line
        return summaries

    def _parse_full_schemas(self) -> Dict[str, str]:
        """Parse full schemas into a dictionary for quick access"""
        schemas = {}
        if not self.schema_content:
            return schemas
            
        collections = self.schema_content.strip().split("Collection: ")[1:]
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            schemas[collection_name] = f"collection: {collection_name}\n{coll_text[name_end+1:]}"
        return schemas

    def normalize_query(self, query: str) -> str:
        """Normalize natural language query using value synonyms"""
        normalized = query.lower()
        for field, synonym_map in self.value_synonyms.items():
            for canonical, variants in synonym_map.items():
                for variant in variants:
                    if variant.lower() in normalized:
                        normalized = normalized.replace(variant.lower(), canonical.lower())
        return normalized

    def _select_best_collection(self, query_text: str) -> Optional[str]:
        """Phase 1: Have LLM select the best collection based on summaries"""
        normalized_query = self.normalize_query(query_text)
        
        print(f"[DEBUG] Normalized query: {normalized_query}")
        
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
            if "collection:" in response.text.lower():
                collection_name = response.text.split("collection:")[1].strip().split()[0].strip('"\'')
                print(f"[INFO] Selected collection: {collection_name}")
                return collection_name
            return None
        except Exception as e:
            print(f"[ERROR] Error parsing collection selection: {str(e)}")
            return None

    def _format_collection_summaries(self) -> str:
        """Format collection summaries for selection prompt"""
        return "\n".join(
            f"- {name}: {summary}" 
            for name, summary in self.schema_summaries.items()
        )

    def _format_index_information(self, collection_name: str) -> str:
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

    def _generate_query_for_collection(self, query_text: str, collection_name: str) -> Dict:
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
            if self.auto_create_indexes and indexes_info.get('needs_indexes', False):
                print(f"[INFO] No indexes found for {collection_name}. Creating recommended indexes...")
                index_creation_result = self.create_recommended_indexes(collection_name)
                print(f"[INFO] Index creation result: {index_creation_result}")
                
                # Refresh index information
                self.collection_indexes = self._get_collection_indexes()
                indexes_info = self.collection_indexes.get(collection_name, {})
                has_indexes = indexes_info.get('has_custom_indexes', False)
        
        index_info = self._format_index_information(collection_name)
        indexed_fields = indexes_info.get('all_indexed_fields', [])
        recommended_fields = indexes_info.get('recommended_single_indexes', [])
        
        # Prepare value synonyms string
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" 
                                    for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])
        
        print(f"[DEBUG] Schema: {schema}")
        print(f"[DEBUG] Index info: {index_info}")
        print(f"[DEBUG] Has custom indexes: {has_indexes}")
        print(f"[DEBUG] Company-scoped collection: {self.access_manager.is_company_scoped(collection_name)}")
            
        # Adjust prompt based on index availability
        if has_indexes:
            optimization_instructions = f"""
OPTIMIZATION PRIORITY (CRITICAL - INDEXES AVAILABLE):
1. **ALWAYS USE INDEXED FIELDS FIRST** - Structure queries to utilize indexed fields: {', '.join(indexed_fields)}
2. **COMPANY FILTERING** - For company-scoped collections, ALWAYS include Cid filter first for optimal performance
3. **COMPOUND INDEX OPTIMIZATION** - When using compound indexes, follow the index field order
4. **RANGE QUERIES** - Use indexed fields for range queries ($gte, $lte, $gt, $lt)
5. **SORTING** - Only sort on indexed fields to avoid in-memory sorts
6. **LIMIT EARLY** - Apply filters on indexed fields before other operations
7. **AGGREGATION OPTIMIZATION** - Place $match stages with indexed fields first in pipeline

AVAILABLE INDEXED FIELDS: {', '.join(indexed_fields)}
"""
        else:
            optimization_instructions = f"""
OPTIMIZATION PRIORITY (NO INDEXES DETECTED):
1. **QUERY SELECTIVITY** - Structure queries to filter data as early as possible
2. **COMPANY FILTERING** - For company-scoped collections, include Cid filter to reduce dataset
3. **AVOID FULL COLLECTION SCANS** - Use specific field matches rather than broad searches
4. **RECOMMENDED INDEXABLE FIELDS** - Consider these fields for best performance: {', '.join(recommended_fields)}
5. **LIMIT RESULTS** - Always limit results when possible to improve performance
6. **SIMPLE OPERATIONS** - Prefer simple find operations over complex aggregations when possible

WARNING: This collection has no custom indexes. Queries may be slow on large datasets.
RECOMMENDED FIELDS TO INDEX: {', '.join(recommended_fields)}
"""

        context_rules = self._get_context_rules(collection_name, query_text)
            
        prompt = f"""
You are a MongoDB query optimization expert. Generate the MOST EFFICIENT query possible.

USER QUERY: "{normalized_query}"

COLLECTION SCHEMA:
{schema}

INDEX INFORMATION:
{index_info}

{optimization_instructions}

CONTEXT RULES (MUST FOLLOW):
{context_rules}
 
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
8. For ObjectId fields, use ObjectId('id_string') format
9. For ObjectId fields, ALWAYS use ObjectId('id_string') format - NEVER use {'$oid' 'id_string'}

PERFORMANCE OPTIMIZATION:
- {"Structure queries to use indexes efficiently, especially Cid for company filtering" if has_indexes else "Structure queries to minimize data scanning, include Cid filter early"}
- {"Avoid queries that can't use indexes (full collection scans)" if has_indexes else "Use the most selective filters first, including company filtering"}
- For aggregations, put {"indexed field filters" if has_indexes else "most selective filters including Cid"} in early $match stages
- {"Use compound indexes in the correct field order" if has_indexes else "Consider which fields would benefit from indexing"}
- Limit results when possible to improve performance

VALID QUERY FORMATS ONLY:
- Optimized find: {{ "field": "value", "field2": "value2" }}
- Optimized regex: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
- Optimized range: {{ "field": {{ "$gte": start, "$lte": end }} }}
- Optimized aggregation: [{{ "$match": {{ "field": criteria }} }}, {{ "$group": {{ "_id": "$field", "count": {{ "$sum": 1 }} }} }}]

Respond ONLY with a JSON object: {{ "collection": "{collection_name}", "query": <optimized_mongo_query>, "optimization_notes": "explanation of optimization approach{"and index usage" if has_indexes else " (no indexes available)"}" }}
"""
        
        try:
            response = self.model.generate_content(prompt)
            response_text = response.text
            
            # Clean up response
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
                "company_scoped": self.access_manager.is_company_scoped(collection_name),
                "index_recommendations": self.get_index_recommendations(collection_name) if not has_indexes else None
            }
            
            return result
        except Exception as e:
            return {"error": f"Failed to parse query: {str(e)}", "raw_response": response.text}

    def _get_context_rules(self, collection_name: str, query_text: str) -> str:
        """Generate context rules based on collection type and query content"""
        rules = []
        
        # Company-level filtering for scoped collections
        if self.access_manager.is_company_scoped(collection_name):
            rules.append(
                f"- MANDATORY: Include company filter 'Cid': ObjectId('{self.company_id}') in ALL queries"
            )
            rules.append(
                f"- PERFORMANCE: Place company filter first for optimal index usage"
            )
        
        # Additional context rules based on query content
        if "my" in query_text.lower() and "userId" in self.full_schemas.get(collection_name, ""):
            rules.append(
                "- Consider adding user-specific filtering if userId field is available"
            )
            
        return "\n".join(rules) if rules else "No additional context filters required"

    def natural_language_to_query(self, natural_language_query: str) -> Dict:
        """Main method to convert NL to optimized MongoDB query with company filtering"""
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

    def _convert_to_case_insensitive(self, query: Any) -> Any:
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

    def _get_operation_type(self, query: Any) -> str:
        """Determine the MongoDB operation type from the query"""
        if isinstance(query, list):
            return "aggregate"
        for op_type in ["find", "insertOne", "insertMany", "updateOne", "updateMany", 
                        "deleteOne", "deleteMany", "countDocuments", "distinct"]:
            if op_type in query:
                return op_type
        return "find"
    


    def execute_query(self, collection_name: str, query: Any) -> Dict:
        """Execute MongoDB query with company-level access control"""
        try:
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            
            # Start timing for performance monitoring
            start_time = time.time()
            
            # Apply company-level filtering
            filtered_query = self.access_manager.inject_company_filter(
                query, self.company_id, collection_name
            )
            
            print(f"\n[DEBUG] Executing {operation_type} on collection '{collection_name}'")
            print(f"[DEBUG] Company ID: {self.company_id}")
            print(f"[DEBUG] Company-scoped: {self.access_manager.is_company_scoped(collection_name)}")
            
            if operation_type == "aggregate":
                pipeline = filtered_query if isinstance(filtered_query, list) else filtered_query.get("aggregate", [])
                
                # Process each $match stage for case-insensitive matching
                processed_pipeline = []
                for stage in pipeline:
                    if "$match" in stage:
                        stage["$match"] = self._convert_to_case_insensitive(stage["$match"])
                    processed_pipeline.append(stage)
                
                print(f"\n[DEBUG] Executing aggregation pipeline:")
                print(json.dumps(processed_pipeline, indent=2, default=str))
                
                # Get explain output using command
                explain_command = {
                    'aggregate': collection_name,
                    'pipeline': processed_pipeline,
                    'explain': True
                }
                explain_output = self.db.command(explain_command)
                print("\n[DEBUG] Query Execution Plan:")
                print(json.dumps(explain_output, indent=2, default=str))
                
                # Execute the actual query
                results = list(collection.aggregate(processed_pipeline, allowDiskUse=True))
                
            elif operation_type == "find":
                filter_query = filtered_query.get("find", filtered_query) if isinstance(filtered_query, dict) else filtered_query
                filter_query = self._convert_to_case_insensitive(filter_query)
                
                print(f"\n[DEBUG] Executing find query:")
                print(json.dumps(filter_query, indent=2, default=str))
                
                # Get explain output for find
                explain_output = collection.find(filter_query).explain()
                print("\n[DEBUG] Query Execution Plan:")
                print(json.dumps(explain_output, indent=2, default=str))
                
                # Execute the actual query
                cursor = collection.find(filter_query)
                results = list(cursor)
            else:
                return {"error": f"Unsupported operation type: {operation_type}"}
            
            execution_time = time.time() - start_time
            print(f"[PERFORMANCE] Query execution completed in {execution_time:.4f} seconds")
            print(f"[PERFORMANCE] Retrieved {len(results)} documents")
            
            return {
                "results": results,
                "count": len(results),
                "query_type": operation_type,
                "execution_time": execution_time,
                "explain_output": explain_output,
                "company_filtered": self.access_manager.is_company_scoped(collection_name)
            }
            
        except Exception as e:
            print(f"[ERROR] Query execution failed: {str(e)}")
            return {"error": str(e)}

    def analyze_query_performance(self, collection_name: str, query: Any) -> Dict:
        """Analyze query performance using MongoDB explain"""
        try:
            collection = self.db[collection_name]
            # Apply company filtering before analysis
            filtered_query = self.access_manager.inject_company_filter(
                query, self.company_id, collection_name
            )
            
            # The execute_query already includes explain output
            result = self.execute_query(collection_name, filtered_query)
            return result.get("explain_output", {})
        except Exception as e:
            return {"error": f"Could not analyze query performance: {str(e)}"}

    def _generate_results_explanation(self, nl_query: str, mongo_query: Any, results: Dict, collection_name: str) -> str:
        """Generate user-friendly results explanation with performance info"""
        result_count = results.get('count', 0)
        sample_results = results.get('results', [])[:3]
        query_type = results.get('query_type', 'find')
        execution_time = results.get('execution_time', 0)
        company_filtered = results.get('company_filtered', False)
        
        company_context = ""
        if company_filtered:
            company_context = "Note: Results are filtered to show only data for your company."
        
        prompt = f"""
You are a helpful MongoDB assistant explaining query results in simple terms.

INSTRUCTIONS:
1. Say Hi 
2. Summarize the key findings from the results
3. Highlight any important numbers or patterns
4. Keep it concise (1-2 short paragraphs max)
5. Use natural, conversational language, but also keep it professional and simple
6. If no results found, suggest possible reasons
7. Tell it in a way that seems natural and human-like, not robotic
8. Avoid technical jargon, use simple terms, and keep it friendly
9. Don't talk about the query used or how it was generated, focus on the results
10. Say the question the user asked in the beginning, in your own words
11. If the query was fast (< 100ms), you can mention it was quick to process

USER ORIGINAL QUESTION: "{nl_query}"

DATABASE COLLECTION: {collection_name}
QUERY TYPE: {query_type}
NUMBER OF RESULTS: {result_count}
EXECUTION TIME: {execution_time:.3f} seconds
COMPANY FILTERED: {company_filtered}
{company_context}

QUERY USED:
{json.dumps(mongo_query, indent=2, default=str)}

SAMPLE RESULTS (first 3):
{json.dumps(sample_results, indent=2, default=str)}
"""
        
        response = self.model.generate_content(prompt)
        return response.text

    def process_query(self, natural_language_query: str, include_explanation: bool = True, 
                     analyze_performance: bool = False, create_indexes_if_needed: Optional[bool] = None) -> Dict:
        """
        Process a natural language query with company-level access control
        
        Args:
            natural_language_query: The natural language query to process
            include_explanation: Whether to include a user-friendly explanation
            analyze_performance: Whether to include performance analysis
            create_indexes_if_needed: Override auto_create_indexes setting
            
        Returns:
            Dict containing query results and metadata
        """
        print(f"\n[PROCESSING] Starting query processing for company {self.company_id}")
        print(f"[PROCESSING] Query: '{natural_language_query}'")
        process_start_time = time.time()

        # Override auto_create_indexes if specified
        if create_indexes_if_needed is not None:
            original_auto_create = self.auto_create_indexes
            self.auto_create_indexes = create_indexes_if_needed

        try:
            # Phase 1 & 2: Get collection and optimized query
            query_gen_start = time.time()
            result = self.natural_language_to_query(natural_language_query)
            query_gen_time = time.time() - query_gen_start
            print(f"[TIMING] Query generation took {query_gen_time:.4f} seconds")
            
            if "error" in result:
                print(f"[ERROR] Query generation failed: {result['error']}")
                return {
                    "status": "error",
                    "message": result["error"],
                    "raw_response": result.get("raw_response", ""),
                    "total_processing_time": time.time() - process_start_time,
                    "company_id": self.company_id
                }

            collection_name = result["collection"]
            print(f'\n[INFO] Selected collection: {collection_name}')
            mongo_query = result["query"]
            
            # Print the generated query in readable format
            print("\n[GENERATED QUERY]")
            if isinstance(mongo_query, list):
                print("Aggregation Pipeline:")
                for i, stage in enumerate(mongo_query, 1):
                    print(f"Stage {i}:")
                    print(json.dumps(stage, indent=2, default=str))
            else:
                print("Find Operation:")
                print(json.dumps(mongo_query, indent=2, default=str))

            # Execute query with company filtering
            query_exec_start = time.time()
            results = self.execute_query(collection_name, mongo_query)
            query_exec_time = time.time() - query_exec_start
            print(f"[TIMING] Query execution took {query_exec_time:.4f} seconds")
            
            # Performance analysis if requested
            performance_info = None
            if analyze_performance:
                perf_start = time.time()
                performance_info = self.analyze_query_performance(collection_name, mongo_query)
                print(f"[TIMING] Performance analysis took {time.time() - perf_start:.4f} seconds")
            
            # Fallback logic for case sensitivity
            if "error" not in results and results.get("count", 0) == 0:
                print("[INFO] No results found, trying case-insensitive version")
                ci_query = self._convert_to_case_insensitive(mongo_query)
                results = self.execute_query(collection_name, ci_query)
                if results.get("count", 0) > 0:
                    mongo_query = ci_query
                    print("[INFO] Case-insensitive query returned results")

            # Build response
            optimization_info = result.get("optimization_info", {})
            has_indexes = optimization_info.get("has_custom_indexes", False)
            
            response = {
                "status": "success" if "error" not in results else "error",
                "company_id": self.company_id,
                "collection": collection_name,
                "generated_query": json.loads(json.dumps(mongo_query, default=str)),
                "optimization_info": json.loads(json.dumps(optimization_info, default=str)),
                "optimization_notes": result.get("optimization_notes", "No optimization information"),
                "has_indexes": has_indexes,
                "company_scoped": self.access_manager.is_company_scoped(collection_name),
                "query_generation_time": query_gen_time,
                "query_execution_time": query_exec_time,
                "total_processing_time": time.time() - process_start_time
            }
            
            # Add index recommendations if no indexes exist
            if not has_indexes:
                response["index_recommendations"] = json.loads(
                    json.dumps(optimization_info.get("index_recommendations"), default=str)
                )
                response["performance_warning"] = "This collection has no custom indexes. Consider creating indexes for better performance."
            
            if performance_info:
                response["performance_analysis"] = json.loads(json.dumps(performance_info, default=str))

            if "error" in results:
                response["message"] = results["error"]
            else:
                response.update({
                    "results": json.loads(json.dumps(results["results"], default=str)),
                    "count": results["count"],
                    "query_type": results["query_type"],
                    "execution_time": results.get("execution_time", 0),
                    "company_filtered": results.get("company_filtered", False)
                })
                
                # Analyze explain output if available
                if "explain_output" in results:
                    explain = results["explain_output"]
                    query_analysis = {}
                    
                    if results["query_type"] == "find":
                        winning_plan = explain.get("queryPlanner", {}).get("winningPlan", {})
                        execution_stats = explain.get("executionStats", {})
                        index_used = winning_plan.get("stage") == "IXSCAN"
                        
                        print("\n[QUERY ANALYSIS]")
                        print(f"Index used: {'Yes' if index_used else 'No'}")
                        if index_used:
                            print(f"Index name: {winning_plan.get('indexName', 'Unknown')}")
                        print(f"Documents examined: {execution_stats.get('totalDocsExamined', 0)}")
                        print(f"Documents returned: {execution_stats.get('nReturned', 0)}")
                        print(f"Execution time (ms): {execution_stats.get('executionTimeMillis', 0)}")
                        
                        query_analysis = {
                            "index_used": index_used,
                            "index_name": winning_plan.get("indexName") if index_used else None,
                            "docs_examined": execution_stats.get("totalDocsExamined", 0),
                            "docs_returned": execution_stats.get("nReturned", 0),
                            "execution_time_ms": execution_stats.get("executionTimeMillis", 0),
                            "winning_plan": json.loads(json.dumps(winning_plan, default=str))
                        }
                    else:
                        stages = explain[0].get("stages", []) if explain else []
                        print("\n[QUERY ANALYSIS] Aggregation Stages:")
                        index_usage = []
                        
                        for stage in stages:
                            stage_info = {
                                "stage_type": stage.get("stage", "Unknown")
                            }
                            print(f"Stage: {stage.get('stage', 'Unknown')}")
                            
                            if "$cursor" in stage:
                                cursor = stage["$cursor"]
                                cursor_plan = cursor.get("queryPlanner", {}).get("winningPlan", {})
                                if cursor_plan.get("stage") == "IXSCAN":
                                    print(f"  Index used: Yes ({cursor_plan.get('indexName', 'Unknown')})")
                                    stage_info["index_used"] = True
                                    stage_info["index_name"] = cursor_plan.get("indexName")
                                else:
                                    print("  Index used: No")
                                    stage_info["index_used"] = False
                                
                                stage_info["execution_stats"] = json.loads(
                                    json.dumps(cursor.get("executionStats", {}), default=str)
                                )
                            
                            index_usage.append(stage_info)
                        
                        query_analysis = {
                            "stages": index_usage
                        }
                    
                    response["query_analysis"] = query_analysis

                if include_explanation:
                    explain_start = time.time()
                    response["explanation"] = self._generate_results_explanation_with_index_context(
                        natural_language_query,
                        mongo_query,
                        {
                            "count": results["count"],
                            "results": results["results"],
                            "query_type": results["query_type"],
                            "execution_time": results.get("execution_time", 0)
                        },
                        collection_name,
                        has_indexes
                    )
                    print(f"[TIMING] Explanation generation took {time.time() - explain_start:.4f} seconds")
            
            return response
            
        finally:
            # Restore original auto_create_indexes setting
            if create_indexes_if_needed is not None:
                self.auto_create_indexes = original_auto_create

    def get_company_info(self) -> Dict:
        """Get information about the current company"""
        try:
            company = self.db.company.find_one({"_id": ObjectId(self.company_id)})
            if company:
                return {
                    "company_id": str(company["_id"]),
                    "name": company.get("name", "Unknown"),
                    "status": company.get("status", "Unknown"),
                    "created_at": company.get("createdAt", "Unknown")
                }
            else:
                return {"error": "Company not found"}
        except Exception as e:
            return {"error": f"Failed to retrieve company info: {str(e)}"}

    def list_accessible_collections(self) -> Dict:
        """List all collections accessible to the current company"""
        try:
            all_collections = self.db.list_collection_names()
            
            accessible = {
                "company_scoped": [],
                "global": [],
                "total": len(all_collections)
            }
            
            for collection in all_collections:
                if self.access_manager.is_company_scoped(collection):
                    accessible["company_scoped"].append(collection)
                else:
                    accessible["global"].append(collection)
            
            accessible["company_scoped_count"] = len(accessible["company_scoped"])
            accessible["global_count"] = len(accessible["global"])
            
            return accessible
            
        except Exception as e:
            return {"error": f"Failed to list collections: {str(e)}"}

    def get_collection_stats(self, collection_name: str) -> Dict:
        """Get statistics for a specific collection with company filtering"""
        try:
            collection = self.db[collection_name]
            
            stats = {
                "collection": collection_name,
                "company_scoped": self.access_manager.is_company_scoped(collection_name)
            }
            
            if self.access_manager.is_company_scoped(collection_name):
                # Get company-specific count
                company_count = collection.count_documents({"Cid": ObjectId(self.company_id)})
                total_count = collection.count_documents({})
                
                stats.update({
                    "company_document_count": company_count,
                    "total_document_count": total_count,
                    "company_percentage": round((company_count / total_count * 100), 2) if total_count > 0 else 0
                })
            else:
                # Global collection
                total_count = collection.count_documents({})
                stats.update({
                    "total_document_count": total_count,
                    "note": "This is a global collection, not company-specific"
                })
            
            # Index information
            indexes = self.collection_indexes.get(collection_name, {})
            stats["indexes"] = {
                "has_custom_indexes": indexes.get("has_custom_indexes", False),
                "single_field_count": len(indexes.get("single_field_indexes", [])),
                "compound_index_count": len(indexes.get("compound_indexes", []))
            }
            
            return stats
            
        except Exception as e:
            return {"error": f"Failed to get collection stats: {str(e)}"}


# Example usage and testing
if __name__ == "__main__":
    # Example usage
    try:
        # Initialize with a specific company ID
        company_id = "64f8c0b2d4f1e3a1b2c3d4e5"  # Replace with actual company ID
        
        query_system = NLToMongoDBQuerySystem(
            company_id=company_id,
            auto_create_indexes=True
        )
        
        print(f"Initialized query system for company: {company_id}")
        
        # Get company info
        company_info = query_system.get_company_info()
        print(f"Company Info: {company_info}")
        
        # List accessible collections
        collections = query_system.list_accessible_collections()
        print(f"Accessible Collections: {collections}")
        
        # Example queries
        test_queries = [
            "Show me all agent dispositions from last month",
            "How many customers do we have in Telangana?",
            "List all active users in our company",
            "Show me the latest report history"
        ]
        
        for query in test_queries:
            print(f"\n{'='*50}")
            print(f"Processing: {query}")
            print('='*50)
            
            result = query_system.process_query(
                query, 
                include_explanation=True,
                analyze_performance=True
            )
            
            if result["status"] == "success":
                print(f"Found {result['count']} results in {result['execution_time']:.3f}s")
                if result.get("explanation"):
                    print(f"Explanation: {result['explanation']}")
            else:
                print(f"Error: {result.get('message', 'Unknown error')}")
                
    except Exception as e:
        print(f"Initialization failed: {e}")
        print("Make sure:")
        print("1. MongoDB is running on localhost:27017")
        print("2. Database 'runo' exists")
        print("3. Company collection has valid records")
        print("4. Schema file exists at 'backend/schema.txt'")
        print("5. Gemini API key is valid")
        
        
        
query_system = NLToMongoDBQuerySystem(
    company_id="67c6da5aa4171809121d2990",
    auto_create_indexes=True)


result = query_system.process_query(
    "Show me all the customers")