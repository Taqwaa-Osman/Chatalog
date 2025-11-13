import pandas as pd
from neo4j import GraphDatabase
import re

class Neo4jConnection:
    def __init__(self, uri, user, password, database="neo4j"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
    
    def close(self):
        self.driver.close()
    
    def execute_query(self, query, parameters=None):
        with self.driver.session(database=self.database) as session:
            result = session.run(query, parameters)
            return [record for record in result]

def clean_text(text):
    """Clean text for Neo4j"""
    if pd.isna(text):
        return ""
    return str(text).strip()

def create_author_relationship(conn, bibnum, author_name):
    """Create WRITTEN_BY relationship between Book and Author"""
    if not author_name or author_name.strip() == "":
        return False
    
    query = """
    MATCH (b:Book {bibnum: $bibnum})
    MERGE (a:Author {name: $author_name})
    MERGE (b)-[:WRITTEN_BY]->(a)
    RETURN b, a
    """
    
    parameters = {
        'author_name': author_name.strip(),
        'bibnum': bibnum
    }
    
    try:
        result = conn.execute_query(query, parameters)
        return len(result) > 0
    except Exception as e:
        print(f"    Error creating author relationship: {e}")
        return False

def create_publisher_relationship(conn, bibnum, publisher_name):
    """Create PUBLISHED_BY relationship between Book and Publisher"""
    if not publisher_name or publisher_name.strip() == "":
        return False
    
    query = """
    MATCH (b:Book {bibnum: $bibnum})
    MERGE (p:Publisher {name: $publisher_name})
    MERGE (b)-[:PUBLISHED_BY]->(p)
    RETURN b, p
    """
    
    parameters = {
        'publisher_name': publisher_name.strip(),
        'bibnum': bibnum
    }
    
    try:
        result = conn.execute_query(query, parameters)
        return len(result) > 0
    except Exception as e:
        print(f"    Error creating publisher relationship: {e}")
        return False

def create_subject_relationship(conn, bibnum, subject_name):
    """Create HAS_SUBJECT relationship between Book and Subject"""
    if not subject_name or subject_name.strip() == "":
        return False
    
    subject_clean = subject_name.strip()
    
    query = """
    MATCH (b:Book {bibnum: $bibnum})
    MERGE (s:Subject {name: $subject_name})
    MERGE (b)-[:HAS_SUBJECT]->(s)
    RETURN b, s
    """
    
    parameters = {
        'subject_name': subject_clean,
        'bibnum': bibnum
    }
    
    try:
        result = conn.execute_query(query, parameters)
        return len(result) > 0
    except Exception as e:
        print(f"    Error creating subject relationship: {e}")
        return False

def create_location_relationship(conn, bibnum, location_code):
    """Create LOCATED_AT relationship between Book and Location"""
    if not location_code or location_code.strip() == "":
        return False
    
    query = """
    MATCH (b:Book {bibnum: $bibnum})
    MERGE (l:Location {code: $location_code})
    MERGE (b)-[:LOCATED_AT]->(l)
    RETURN b, l
    """
    
    parameters = {
        'location_code': location_code.strip(),
        'bibnum': bibnum
    }
    
    try:
        result = conn.execute_query(query, parameters)
        return len(result) > 0
    except Exception as e:
        print(f"    Error creating location relationship: {e}")
        return False

def add_all_relationships(conn, df):
    """Add all relationships to existing nodes"""
    
    print("\n1. Creating WRITTEN_BY relationships (Book -> Author)...")
    author_count = 0
    for idx, row in df.iterrows():
        if pd.notna(row['author']):
            if create_author_relationship(conn, row['bibnum'], row['author']):
                author_count += 1
        if (idx + 1) % 10 == 0:
            print(f"  Processed {idx + 1} books...")
    print(f"✓ Created {author_count} WRITTEN_BY relationships")
    
    print("\n2. Creating PUBLISHED_BY relationships (Book -> Publisher)...")
    publisher_count = 0
    for idx, row in df.iterrows():
        if pd.notna(row['publisher']):
            if create_publisher_relationship(conn, row['bibnum'], row['publisher']):
                publisher_count += 1
        if (idx + 1) % 10 == 0:
            print(f"  Processed {idx + 1} books...")
    print(f"✓ Created {publisher_count} PUBLISHED_BY relationships")
    
    print("\n3. Creating HAS_SUBJECT relationships (Book -> Subject)...")
    subject_count = 0
    subject_cols = [col for col in df.columns if col.startswith('subject_')]
    
    for idx, row in df.iterrows():
        bibnum = row['bibnum']
        
        for col in subject_cols:
            if pd.notna(row[col]) and row[col].strip() != "":
                if create_subject_relationship(conn, bibnum, row[col]):
                    subject_count += 1
        
        if (idx + 1) % 10 == 0:
            print(f"  Processed {idx + 1} books...")
    print(f"✓ Created {subject_count} HAS_SUBJECT relationships")
    
    print("\n4. Creating LOCATED_AT relationships (Book -> Location)...")
    location_count = 0
    for idx, row in df.iterrows():
        if pd.notna(row['itemlocation']):
            if create_location_relationship(conn, row['bibnum'], row['itemlocation']):
                location_count += 1
        if (idx + 1) % 10 == 0:
            print(f"  Processed {idx + 1} books...")
    print(f"✓ Created {location_count} LOCATED_AT relationships")
    
    return {
        'author': author_count,
        'publisher': publisher_count,
        'subject': subject_count,
        'location': location_count
    }

def main():
    print("="*60)
    print("ADD RELATIONSHIPS TO KNOWLEDGE GRAPH")
    print("="*60)
    
    # Configuration - UPDATE THESE VALUES
    CSV_PATH = 'fantasy_books.csv'
    
    NEO4J_URI = "bolt://127.0.0.1:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "oooooooo"  # CHANGE THIS
    NEO4J_DATABASE = "chatalogkg"  # Your database name
    
    print(f"\nConfiguration:")
    print(f"  CSV Path: {CSV_PATH}")
    print(f"  Neo4j URI: {NEO4J_URI}")
    print(f"  Neo4j Database: {NEO4J_DATABASE}")
    
    # Load data
    print(f"\nLoading data from CSV...")
    try:
        df = pd.read_csv(CSV_PATH)
        print(f"✓ Loaded {len(df)} books")
    except FileNotFoundError:
        print(f"✗ ERROR: Could not find CSV file at: {CSV_PATH}")
        return
    except Exception as e:
        print(f"✗ ERROR loading CSV: {e}")
        return
    
    # Connect to Neo4j
    print(f"\nConnecting to Neo4j...")
    try:
        conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE)
        print("✓ Connected successfully!")
    except Exception as e:
        print(f"✗ ERROR connecting to Neo4j: {e}")
        return
    
    # Check existing nodes
    print(f"\nChecking existing nodes...")
    try:
        node_counts = conn.execute_query("MATCH (n) RETURN labels(n)[0] as type, count(n) as count")
        print("Current node counts:")
        for record in node_counts:
            print(f"  - {record['type']}: {record['count']}")
        
        rel_count = conn.execute_query("MATCH ()-[r]->() RETURN count(r) as count")
        print(f"Current relationships: {rel_count[0]['count']}")
    except Exception as e:
        print(f"⚠ Could not check existing data: {e}")
    
    # Add relationships
    print("\n" + "="*60)
    print("CREATING RELATIONSHIPS...")
    print("="*60)
    
    try:
        counts = add_all_relationships(conn, df)
    except Exception as e:
        print(f"\n✗ ERROR creating relationships: {e}")
        conn.close()
        return
    
    # Final statistics
    print("\n" + "="*60)
    print("CHECKING FINAL RESULTS...")
    print("="*60)
    
    try:
        final_rel_count = conn.execute_query("MATCH ()-[r]->() RETURN count(r) as count")
        rel_by_type = conn.execute_query("""
            MATCH ()-[r]->()
            RETURN type(r) as relationship_type, count(r) as count
            ORDER BY count DESC
        """)
        
        print(f"\nTotal relationships created: {final_rel_count[0]['count']}")
        print("\nRelationships by type:")
        for record in rel_by_type:
            print(f"  - {record['relationship_type']}: {record['count']}")
    except Exception as e:
        print(f"⚠ Could not gather final stats: {e}")
    
    print("\n" + "="*60)
    print("✓ RELATIONSHIPS ADDED SUCCESSFULLY!")
    print("="*60)
    print("\nNext steps:")
    print("  1. Open Neo4j Browser: http://localhost:7474")
    print("  2. Run: MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 50")
    print("  3. You should see your graph with connections!")
    print("="*60)
    
    conn.close()

if __name__ == "__main__":
    main()
