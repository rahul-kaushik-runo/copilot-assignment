import json
import google.generativeai as genai
from pymongo import MongoClient
import time
from difflib import get_close_matches
import os
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
import re




class DatabaseSchema:
    """Handles database schema information and field descriptions"""
    
    def __init__(self, collection: Collection, schema_description: Optional[str] = None):
        self.collection = collection
        self.schema_description = schema_description
        self.indexes = self._get_indexes()
        
        if not schema_description:
            self.schema_info = self._analyze_schema()
        else:
            self.schema_info = self._parse_schema_description(schema_description)
    
    def _parse_schema_description(self, schema_description: str) -> Dict[str, Any]:
        """Parse the provided schema description into structured format"""
        schema = {}
        lines = schema_description.split('\n')
        
        current_field = None
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if ':' in line and not line.startswith(' '):
                field_part = line.split(':')[0].strip()
                description_part = line.split(':', 1)[1].strip()
                field_name = field_part.replace('**', '').replace('`', '')
                
                schema[field_name] = {
                    "description": description_part,
                    "type": self._extract_type_from_description(description_part),
                    "nested": '.' in field_name or 'nested' in description_part.lower()
                }
                current_field = field_name
            elif current_field and line.startswith(' ') and ':' in line:
                subfield_part = line.split(':')[0].strip()
                subdesc_part = line.split(':', 1)[1].strip()
                subfield_name = subfield_part.replace('**', '').replace('`', '')
                
                schema[subfield_name] = {
                    "description": subdesc_part,
                    "type": self._extract_type_from_description(subdesc_part),
                    "nested": '.' in subfield_name,
                    "parent": current_field
                }
        
        return schema
    
    def _extract_type_from_description(self, description: str) -> str:
        """Extract data type from field description"""
        description_lower = description.lower()
        
        if 'objectid' in description_lower:
            return 'ObjectId'
        elif 'timestamp' in description_lower or 'unix' in description_lower:
            return 'int'
        elif 'boolean' in description_lower or 'bool' in description_lower:
            return 'bool'
        elif 'integer' in description_lower or 'int' in description_lower:
            return 'int'
        elif 'float' in description_lower or 'decimal' in description_lower:
            return 'float'
        elif 'array' in description_lower or 'list' in description_lower:
            return 'list'
        elif 'nested' in description_lower or 'dictionary' in description_lower:
            return 'dict'
        else:
            return 'str'
    
    def _analyze_schema(self) -> Dict[str, Any]:
        """Fallback: Analyze collection schema by sampling documents"""
        try:
            sample_docs = list(self.collection.aggregate([{"$sample": {"size": 100}}]))
            
            if not sample_docs:
                return {}
            
            schema = {}
            for doc in sample_docs:
                self._analyze_document(doc, schema)
            
            return schema
        except Exception as e:
            logger.error(f"Error analyzing schema: {e}")
            return {}
    
    def _analyze_document(self, doc: Dict[str, Any], schema: Dict[str, Any], prefix: str = ""):
        """Recursively analyze document structure"""
        for key, value in doc.items():
            field_path = f"{prefix}.{key}" if prefix else key
            
            if field_path not in schema:
                schema[field_path] = {
                    "type": type(value).__name__,
                    "examples": [],
                    "nested": False
                }
            
            if len(schema[field_path]["examples"]) < 5:
                if isinstance(value, (str, int, float, bool)):
                    schema[field_path]["examples"].append(value)
            
            if isinstance(value, dict):
                schema[field_path]["nested"] = True
                self._analyze_document(value, schema, field_path)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                schema[field_path]["nested"] = True
                self._analyze_document(value[0], schema, field_path)
    
    def _get_indexes(self) -> List[Dict[str, Any]]:
        """Get existing indexes from the collection"""
        try:
            return list(self.collection.list_indexes())
        except Exception as e:
            logger.error(f"Error getting indexes: {e}")
            return []
    
    def get_schema_description(self) -> str:
        """Get the schema description for LLM consumption"""
        if self.schema_description:
            description = self.schema_description + "\n\n"
        else:
            description = f"Collection: {self.collection.name}\n\n"
            description += "Fields:\n"
            
            for field, info in self.schema_info.items():
                description += f"- {field} ({info['type']})"
                if 'description' in info:
                    description += f": {info['description']}"
                elif info.get('examples'):
                    examples = ", ".join(str(ex) for ex in info['examples'][:3])
                    description += f" - Examples: {examples}"
                description += "\n"
        
        description += "\nExisting Indexes:\n"
        for idx in self.indexes:
            if idx['name'] != '_id_':
                keys = ", ".join([f"{k}: {v}" for k, v in idx['key'].items()])
                description += f"- {idx['name']}: {keys}\n"
        
        return description

class IndexOptimizer:
    """Handles index analysis and optimization suggestions"""
    
    def __init__(self, collection: Collection, schema: DatabaseSchema):
        self.collection = collection
        self.schema = schema
    
    def analyze_query_for_indexes(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze MongoDB query to suggest optimal indexing strategy"""
        suggestions = {
            "recommended_indexes": [],
            "existing_usable_indexes": [],
            "optimization_notes": []
        }
        
        query_fields = self._extract_query_fields(query)
        sort_fields = self._extract_sort_fields(query)
        
        for index in self.schema.indexes:
            if self._can_use_index(index, query_fields, sort_fields):
                suggestions["existing_usable_indexes"].append(index['name'])
        
        if not suggestions["existing_usable_indexes"]:
            recommended = self._suggest_new_indexes(query_fields, sort_fields)
            suggestions["recommended_indexes"] = recommended
        
        suggestions["optimization_notes"] = self._generate_optimization_notes(query, query_fields, sort_fields)
        
        return suggestions
    
    def _extract_query_fields(self, query: Dict[str, Any]) -> List[str]:
        """Extract fields used in query conditions"""
        fields = []
        
        def extract_from_dict(d: Dict[str, Any], prefix: str = ""):
            for key, value in d.items():
                if key.startswith('$'):
                    if isinstance(value, dict):
                        extract_from_dict(value, prefix)
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                extract_from_dict(item, prefix)
                else:
                    field_path = f"{prefix}.{key}" if prefix else key
                    fields.append(field_path)
                    if isinstance(value, dict):
                        extract_from_dict(value, field_path)
        
        extract_from_dict(query)
        return list(set(fields))
    
    def _extract_sort_fields(self, query: Dict[str, Any]) -> List[str]:
        """Extract fields used in sorting"""
        sort_fields = []
        return sort_fields
    
    def _can_use_index(self, index: Dict[str, Any], query_fields: List[str], sort_fields: List[str]) -> bool:
        """Check if an existing index can be used for the query"""
        try:
            if not index.get('key'):
                return False
            index_fields = list(index['key'].keys())
            return any(field in query_fields for field in index_fields)
        except Exception as e:
            logger.warning(f"Error checking index usability: {e}")
            return False
    
    def _suggest_new_indexes(self, query_fields: List[str], sort_fields: List[str]) -> List[Dict[str, Any]]:
        """Suggest new indexes based on query patterns"""
        suggestions = []
        
        for field in query_fields:
            suggestions.append({
                "fields": {field: 1},
                "type": "single_field",
                "reason": f"Optimize queries on {field}"
            })
        
        if len(query_fields) > 1:
            compound_fields = {field: 1 for field in query_fields[:2]}
            suggestions.append({
                "fields": compound_fields,
                "type": "compound",
                "reason": f"Optimize compound queries on {', '.join(query_fields[:2])}"
            })
        
        return suggestions
    
    def _generate_optimization_notes(self, query: Dict[str, Any], query_fields: List[str], sort_fields: List[str]) -> List[str]:
        """Generate optimization notes for the query"""
        notes = []
        
        if len(query_fields) > 3:
            notes.append("Query uses many fields - consider compound indexes")
        
        if any(field.count('.') > 2 for field in query_fields):
            notes.append("Deep nested field queries may benefit from specific indexing")
        
        query_str = str(query)
        if '$regex' in query_str or '$text' in query_str:
            notes.append("Text/regex queries benefit from text indexes")
        
        if '$gte' in query_str or '$lte' in query_str or '$gt' in query_str or '$lt' in query_str:
            notes.append("Range queries benefit from B-tree indexes")
        
        return notes