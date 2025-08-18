class PromptExtractor:
    def __init__(self, schemas_str):
        self.SCHEMAS_STR = schemas_str.strip()  # Assign before creating summaries
        self.schema_summaries = self._create_schema_summaries()

    def _format_collection_summaries(self):
        """Format collection summaries for selection prompt"""
        return "\n".join(
            f"- {name}: {summary}" 
            for name, summary in self.schema_summaries.items()
        )

    def _create_schema_summaries(self):
        """Create concise one-line summaries of each collection"""
        summaries = {}
        collections = self.SCHEMAS_STR.strip().split("Collection: ")[1:]
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            first_line = coll_text[name_end+1:].split('\n')[0].strip()
            summaries[collection_name] = first_line
        return summaries

    def normalize_query(self, query):
        """Simple query normalization placeholder"""
        return query.strip().lower()

    def _identify_lookup_requirements(self, query_text):
        """Dummy placeholder for relationship identification"""
        return None, []

    def _select_best_collection(self, query_text):
        """Generate the final prompt for LLM to choose the best collection"""
        primary_collection, required_lookups = self._identify_lookup_requirements(query_text)

        if primary_collection and primary_collection in self.schema_summaries:
            return primary_collection

        normalized_query = self.normalize_query(query_text)

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
4. If the query mentions multiple entities with "with" or "and" (e.g., "users with their call interactions"),
   select the collection that represents the MAIN entity (before "with").
5. Respond ONLY with the collection name in this format: "collection: <name>"

EXAMPLE RESPONSES:
- "collection: report-agent-disposition"
- "collection: user"
"""
        print(prompt)


# -------- MAIN EXECUTION --------

if __name__ == "__main__":
    with open("backend/schema.txt", "r", encoding="utf-8") as f:
        schema_content = f.read()

    extractor = PromptExtractor(schema_content)
    extractor._select_best_collection("I want to see the details of my missed calls in the system.")
