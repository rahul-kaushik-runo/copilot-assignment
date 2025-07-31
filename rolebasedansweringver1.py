from backend.query_system import NLToMongoDBQuerySystem  



obj = NLToMongoDBQuerySystem()
print(obj._create_schema_summaries())