from pymongo import MongoClient
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt

def analyze_collection_relationships(db):
    """Analyze relationships between collections in a MongoDB database."""
    collection_names = db.list_collection_names()
    relationships = defaultdict(list)
    G = nx.DiGraph()
    
    for collection in collection_names:
        G.add_node(collection)
    
    for collection_name in collection_names:
        collection = db[collection_name]
        sample_docs = collection.find().limit(100)
        
        for doc in sample_docs:
            for field, value in doc.items():
                if field.endswith('_id') or field.endswith('Id') or field.endswith('ID'):
                    possible_target = field[:-3] if field.endswith('_id') else field[:-2]
                    if possible_target in collection_names:
                        relationship = f"has many {collection_name} references {possible_target} via {field}"
                        relationships[collection_name].append(relationship)
                        G.add_edge(collection_name, possible_target, label=field)
                elif isinstance(value, dict):
                    if '_id' in value and isinstance(value['_id'], (str, int)):
                        possible_target = field
                        if possible_target in collection_names:
                            relationship = f"has embedded {field} documents in {collection_name}"
                            relationships[collection_name].append(relationship)
                            G.add_edge(collection_name, possible_target, label=f"embeds {field}")
        
        first_doc = collection.find_one()
        if first_doc:
            for field, value in first_doc.items():
                if isinstance(value, list) and len(value) > 0:
                    first_item = value[0]
                    if isinstance(first_item, (str, int)):
                        possible_target = field.rstrip('s')
                        if possible_target in collection_names:
                            relationship = f"has array of {possible_target} references in {collection_name}.{field}"
                            relationships[collection_name].append(relationship)
                            G.add_edge(collection_name, possible_target, label=f"array:{field}")
                    elif isinstance(first_item, dict) and '_id' in first_item:
                        possible_target = field.rstrip('s')
                        if possible_target in collection_names:
                            relationship = f"has array of embedded {possible_target} documents in {collection_name}.{field}"
                            relationships[collection_name].append(relationship)
                            G.add_edge(collection_name, possible_target, label=f"embeds array:{field}")
    
    return dict(relationships), G

def visualize_relationships(graph):
    """Save the relationship graph to a file."""
    plt.figure(figsize=(12, 10))
    pos = nx.spring_layout(graph, k=0.5, iterations=50)
    edge_labels = nx.get_edge_attributes(graph, 'label')
    
    nx.draw_networkx_nodes(graph, pos, node_size=3000, node_color='lightblue', alpha=0.9)
    nx.draw_networkx_edges(graph, pos, width=1.0, alpha=0.5, edge_color='gray')
    nx.draw_networkx_labels(graph, pos, font_size=10, font_family='sans-serif')
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels, font_size=8)
    
    plt.title("Database Collection Relationships")
    plt.axis('off')
    plt.tight_layout()
    plt.savefig('database_relationships.png')
    print("Relationship graph saved to database_relationships.png")

def print_relationships(relationships):
    """Print the discovered relationships."""
    print("\nDatabase Collection Relationships:")
    print("=" * 50)
    for collection, rels in relationships.items():
        print(f"\nCollection: {collection}")
        if rels:
            for rel in rels:
                print(f"  - {rel}")
        else:
            print("  - No explicit relationships found")
    print("\n")

def main():
    client = MongoClient('localhost', 27017)
    db = client['runo']  # Replace with your database name
    
    relationships, graph = analyze_collection_relationships(db)
    print_relationships(relationships)
    visualize_relationships(graph)

if __name__ == "__main__":
    main()