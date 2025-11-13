from neo4j import GraphDatabase

NEO4J_URI = "bolt://127.0.0.1:7687"  # or "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_DATABASE = "chatalogkg"  # Your database name

# List of passwords to try
passwords_to_try = [
    "neo4j",        # Default
    "password",     # Common
    "",             # Empty
    "test",         # Common
    "admin",        # Common
    "12345678",     # Common
]

print("="*60)
print("NEO4J PASSWORD FINDER")
print("="*60)
print(f"\nTrying to connect to: {NEO4J_URI}")
print(f"Database: {NEO4J_DATABASE}")
print(f"Username: {NEO4J_USER}\n")

success = False

for password in passwords_to_try:
    try:
        print(f"Trying password: '{password}'...", end=" ")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, password))
        
        # Try to run a simple query
        with driver.session(database=NEO4J_DATABASE) as session:
            result = session.run("RETURN 1 as test")
            result.single()
        
        driver.close()
        
        print("✓ SUCCESS!")
        print("\n" + "="*60)
        print(f"FOUND IT! Your password is: '{password}'")
        print("="*60)
        print(f"\nUpdate your build_kg_v2.py script with:")
        print(f'NEO4J_PASSWORD = "{password}"')
        print("="*60)
        success = True
        break
        
    except Exception as e:
        print(f"✗ Failed")
        # print(f"  Error: {e}")  # Uncomment to see error details

if not success:
    print("\n" + "="*60)
    print("None of the common passwords worked.")
    print("="*60)
    print("\nYour password might be custom. Try:")
    print("1. Check Neo4j Desktop settings")
    print("2. Reset the password in Neo4j Desktop")
    print("3. Or provide your custom password to test")
    print("\nTo test a custom password, run:")
    print('python test_neo4j_connection.py "your_password_here"')
    print("="*60)
