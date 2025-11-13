import pandas as pd
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
import numpy as np
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

def clean_year(year_text):
    """Remove brackets from years while preserving the value"""
    if pd.isna(year_text):
        return ""
    year_str = str(year_text).strip()
    # Remove brackets if present
    year_str = re.sub(r'[\[\]]', '', year_str)
    return year_str

def is_year_inferred(year_text):
    """Check if year has brackets indicating it was inferred"""
    if pd.isna(year_text):
        return False
    return '[' in str(year_text)

def clear_database(conn):
    """Clear all nodes and relationships"""
    query = "MATCH (n) DETACH DELETE n"
    conn.execute_query(query)
    print("✓ Database cleared!")

def create_book_node(conn, row):
    """Create a Book node with both original and cleaned publication year"""
    query = """
    CREATE (b:Book {
        bibnum: $bibnum,
        title: $title,
        isbn: $isbn,
        publication_year_original: $pub_year_orig,
        publication_year: $pub_year_clean,
        year_inferred: $year_inferred,
        item_type: $item_type,
        item_collection: $item_collection,
        floating_item: $floating_item,
        item_count: $item_count,
        report_date: $report_date
    })
    RETURN b
    """
    
    pub_year_orig = clean_text(row['publicationyear'])
    pub_year_clean = clean_year(row['publicationyear'])
    year_inferred = is_year_inferred(row['publicationyear'])
    
    parameters = {
        'bibnum': clean_text(row['bibnum']),
        'title': clean_text(row['title']),
        'isbn': clean_text(row['isbn']),
        'pub_year_orig': pub_year_orig,           # "[2017]" or "2017"
        'pub_year_clean': pub_year_clean,         # "2017" in both cases
        'year_inferred': year_inferred,           # True if had brackets
        'item_type': clean_text(row['itemtype']),
        'item_collection': clean_text(row['itemcollection']),
        'floating_item': clean_text(row['floatingitem']),
        'item_count': int(row['itemcount']) if pd.notna(row['itemcount']) else 0,
        'report_date': clean_text(row['reportdate'])
    }
    
    conn.execute_query(query, parameters)

def populate_books(conn, df):
    """Create all book nodes"""
    print("Creating book nodes...")
    for idx, row in df.iterrows():
        create_book_node(conn, row)
        if (idx + 1) % 5 == 0:
            print(f"  Created {idx + 1} books...")
    print(f"✓ Created {len(df)} book nodes")

def create_author_and_link(conn, bibnum, author_name):
    """Create Author node and link to Book"""
    if not author_name or author_name.strip() == "":
        return
    
    query = """
    MERGE (a:Author {name: $author_name})
    WITH a
    MATCH (b:Book {bibnum: $bibnum})
    MERGE (b)-[:WRITTEN_BY]->(a)
    """
    
    parameters = {
        'author_name': author_name.strip(),
        'bibnum': bibnum
    }
    
    conn.execute_query(query, parameters)

def populate_authors(conn, df):
    """Create all authors and relationships"""
    print("Creating author nodes and relationships...")
    for idx, row in df.iterrows():
        if pd.notna(row['author']):
            create_author_and_link(conn, row['bibnum'], row['author'])
        if (idx + 1) % 5 == 0:
            print(f"  Processed {idx + 1} authors...")
    print("✓ Created author nodes and relationships")

def create_publisher_and_link(conn, bibnum, publisher_name):
    """Create Publisher node and link to Book"""
    if not publisher_name or publisher_name.strip() == "":
        return
    
    query = """
    MERGE (p:Publisher {name: $publisher_name})
    WITH p
    MATCH (b:Book {bibnum: $bibnum})
    MERGE (b)-[:PUBLISHED_BY]->(p)
    """
    
    parameters = {
        'publisher_name': publisher_name.strip(),
        'bibnum': bibnum
    }
    
    conn.execute_query(query, parameters)

def populate_publishers(conn, df):
    """Create all publishers and relationships"""
    print("Creating publisher nodes and relationships...")
    for idx, row in df.iterrows():
        if pd.notna(row['publisher']):
            create_publisher_and_link(conn, row['bibnum'], row['publisher'])
        if (idx + 1) % 5 == 0:
            print(f"  Processed {idx + 1} publishers...")
    print("✓ Created publisher nodes and relationships")

def create_subject_and_link(conn, bibnum, subject_name):
    """Create Subject node and link to Book"""
    if not subject_name or subject_name.strip() == "":
        return
    
    subject_clean = subject_name.strip()
    
    query = """
    MERGE (s:Subject {name: $subject_name})
    WITH s
    MATCH (b:Book {bibnum: $bibnum})
    MERGE (b)-[:HAS_SUBJECT]->(s)
    """
    
    parameters = {
        'subject_name': subject_clean,
        'bibnum': bibnum
    }
    
    conn.execute_query(query, parameters)

def populate_subjects(conn, df):
    """Create all subject nodes and relationships"""
    print("Creating subject nodes and relationships...")
    
    subject_cols = [col for col in df.columns if col.startswith('subject_')]
    
    for idx, row in df.iterrows():
        bibnum = row['bibnum']
        
        for col in subject_cols:
            if pd.notna(row[col]) and row[col].strip() != "":
                create_subject_and_link(conn, bibnum, row[col])
        
        if (idx + 1) % 5 == 0:
            print(f"  Processed subjects for {idx + 1} books...")
    
    print("✓ Created subject nodes and relationships")

def create_location_and_link(conn, bibnum, location_code):
    """Create Location node and link to Book"""
    if not location_code or location_code.strip() == "":
        return
    
    query = """
    MERGE (l:Location {code: $location_code})
    WITH l
    MATCH (b:Book {bibnum: $bibnum})
    MERGE (b)-[:LOCATED_AT]->(l)
    """
    
    parameters = {
        'location_code': location_code.strip(),
        'bibnum': bibnum
    }
    
    conn.execute_query(query, parameters)

def populate_locations(conn, df):
    """Create all location nodes and relationships"""
    print("Creating location nodes and relationships...")
    for idx, row in df.iterrows():
        if pd.notna(row['itemlocation']):
            create_location_and_link(conn, row['bibnum'], row['itemlocation'])
        if (idx + 1) % 5 == 0:
            print(f"  Processed {idx + 1} locations...")
    print("✓ Created location nodes and relationships")

def create_book_embedding(conn, bibnum, title, embedding_vector):
    """Add embedding to Book node"""
    query = """
    MATCH (b:Book {bibnum: $bibnum})
    SET b.title_embedding = $embedding,
        b.embedding_text = $title
    RETURN b
    """
    
    parameters = {
        'bibnum': bibnum,
        'embedding': embedding_vector.tolist(),
        'title': title
    }
    
    conn.execute_query(query, parameters)

def add_embeddings_to_books(conn, df, model):
    """Create and store embeddings for all books"""
    print("Creating embeddings for books...")
    
    for idx, row in df.iterrows():
        title = clean_text(row['title'])
        
        # Create embedding
        embedding = model.encode(title)
        
        # Store in Neo4j
        create_book_embedding(conn, row['bibnum'], title, embedding)
        
        if (idx + 1) % 5 == 0:
            print(f"  Created embeddings for {idx + 1} books...")
    
    print("✓ Added embeddings to all books")

def print_sample_queries():
    """Print example queries to help users get started"""
    print("\n" + "="*60)
    print("SAMPLE QUERIES TO TRY IN NEO4J BROWSER")
    print("="*60)
    print("""
1. View all nodes:
   MATCH (n) RETURN n LIMIT 50

2. Count nodes by type:
   MATCH (n) RETURN labels(n) as Type, count(n) as Count

3. Find books from 2017 (including inferred years):
   MATCH (b:Book {publication_year: "2017"})
   RETURN b.title, b.publication_year_original, b.year_inferred

4. Find only books with inferred publication years:
   MATCH (b:Book {year_inferred: true})
   RETURN b.title, b.publication_year_original
   LIMIT 10

5. Find books from 2017-2020:
   MATCH (b:Book)
   WHERE toInteger(b.publication_year) >= 2017 
     AND toInteger(b.publication_year) <= 2020
   RETURN b.title, b.publication_year_original
   ORDER BY b.publication_year

6. Find similar books by shared subjects:
   MATCH (b1:Book {title: "The Ickabog"})-[:HAS_SUBJECT]->(s:Subject)<-[:HAS_SUBJECT]-(b2:Book)
   WHERE b1 <> b2
   RETURN b2.title, count(s) as shared_subjects
   ORDER BY shared_subjects DESC
   LIMIT 5

7. Find books with dragons AND magic:
   MATCH (b:Book)-[:HAS_SUBJECT]->(s1:Subject)
   MATCH (b)-[:HAS_SUBJECT]->(s2:Subject)
   WHERE s1.name CONTAINS "Dragon" AND s2.name CONTAINS "Magic"
   RETURN b.title, b.publication_year_original
    """)
    print("="*60)

def main():
    print("="*60)
    print("FANTASY BOOKS KNOWLEDGE GRAPH BUILDER v2")
    print("(With preserved year metadata)")
    print("="*60)
    
    # Configuration - UPDATE THESE VALUES
    CSV_PATH = 'fantasy_books.csv'  # Change to your CSV location
    NUM_BOOKS = 158  # Change to 158 for full dataset, or None for all books
    
    NEO4J_URI = "bolt://127.0.0.1:7687"  # or "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "oooooooo"  # CHANGE THIS to your actual password
    NEO4J_DATABASE = "chatalogkg"  # Change if using different database name
    
    print(f"\nConfiguration:")
    print(f"  CSV Path: {CSV_PATH}")
    print(f"  Number of books: {NUM_BOOKS if NUM_BOOKS else 'ALL'}")
    print(f"  Neo4j URI: {NEO4J_URI}")
    print(f"  Neo4j Database: {NEO4J_DATABASE}")
    
    # Load data
    print(f"\n1. Loading data from CSV...")
    try:
        df = pd.read_csv(CSV_PATH)
        if NUM_BOOKS:
            df_subset = df.head(NUM_BOOKS)
        else:
            df_subset = df
        print(f"   ✓ Loaded {len(df_subset)} books for processing")
    except FileNotFoundError:
        print(f"   ✗ ERROR: Could not find CSV file at: {CSV_PATH}")
        print(f"   Please update CSV_PATH in the script or move the file.")
        return
    except Exception as e:
        print(f"   ✗ ERROR loading CSV: {e}")
        return
    
    # Connect to Neo4j
    print(f"\n2. Connecting to Neo4j...")
    try:
        conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE)
        print("   ✓ Connected successfully!")
    except Exception as e:
        print(f"   ✗ ERROR connecting to Neo4j: {e}")
        print(f"   Please check:")
        print(f"   - Neo4j Desktop is running")
        print(f"   - Your password is correct")
        print(f"   - Database name is correct")
        return
    
    # Clear existing data
    print(f"\n3. Clearing existing database...")
    try:
        clear_database(conn)
    except Exception as e:
        print(f"   ⚠ Warning: Could not clear database: {e}")
        response = input("   Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return
    
    # Build KG
    print(f"\n4. Building Knowledge Graph...")
    try:
        populate_books(conn, df_subset)
        populate_authors(conn, df_subset)
        populate_publishers(conn, df_subset)
        populate_subjects(conn, df_subset)
        populate_locations(conn, df_subset)
    except Exception as e:
        print(f"   ✗ ERROR building graph: {e}")
        conn.close()
        return
    
    # Add embeddings
    print(f"\n5. Creating embeddings...")
    try:
        print("   Loading embedding model (this may take a minute)...")
        model = SentenceTransformer('all-MiniLM-L6-v2')
        add_embeddings_to_books(conn, df_subset, model)
    except Exception as e:
        print(f"   ✗ ERROR creating embeddings: {e}")
        print(f"   The graph was created but without embeddings.")
        print(f"   You can still use it, but vector search won't work.")
    
    # Get stats
    print(f"\n6. Gathering statistics...")
    try:
        stats = conn.execute_query("MATCH (n) RETURN labels(n) as type, count(n) as count")
        inferred_count = conn.execute_query("MATCH (b:Book {year_inferred: true}) RETURN count(b) as count")
    except Exception as e:
        print(f"   ⚠ Could not gather stats: {e}")
        stats = []
        inferred_count = []
    
    # Done!
    print("\n" + "="*60)
    print("✓ KNOWLEDGE GRAPH BUILT SUCCESSFULLY!")
    print("="*60)
    
    if stats:
        print(f"\nNode Statistics:")
        for record in stats:
            print(f"  - {record['type'][0]}: {record['count']}")
    
    if inferred_count and len(inferred_count) > 0:
        print(f"\nYear Metadata:")
        print(f"  - Books with inferred years (had brackets): {inferred_count[0]['count']}")
    
    print(f"\nNext steps:")
    print("  1. Open Neo4j Browser: http://localhost:7474")
    print("  2. Select database: " + NEO4J_DATABASE)
    print("  3. Try the sample queries below!")
    
    print_sample_queries()
    
    conn.close()

if __name__ == "__main__":
    main()
