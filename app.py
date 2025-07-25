from flask import Flask, request, jsonify, render_template
from backend.query_system import NLToMongoDBQuerySystem  
# Import the query system class

import json 
app = Flask(__name__)
nl_to_mongodb = NLToMongoDBQuerySystem()  # Initialize query system

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/query', methods=['POST'])
def process_query():
    data = request.json
    if not data or 'query' not in data:
        return jsonify({'status': 'error', 'message': 'No query provided'}), 400
    
    natural_language_query = data['query']
    include_explanation = data.get('include_explanation', True)
    
    result = nl_to_mongodb.process_query(natural_language_query, include_explanation)
    return jsonify(result)

@app.route('/api/history', methods=['GET'])
def get_history():
    return jsonify({
        'status': 'success',
        'history': [
            {'query': q, 'mongodb_query': m} 
            for q, m in nl_to_mongodb.query_history
        ]
    })

# @app.route('/api/schema', methods=['GET'])
# def get_schema():
#     schema = nl_to_mongodb.collection_schemas()
#     fields = nl_to_mongodb.get_collection_fields()
#     return jsonify({
#         'status': 'success',
#         'schema': json.loads(schema) if schema else {},
#         'fields': fields.split(', ') if fields else []
#     })


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
