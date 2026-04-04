"""
Chatalog FastAPI Backend
Run with: uvicorn api:app --reload
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase
from datetime import datetime
import uuid
import time
import hashlib

from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE

app = FastAPI(title="Chatalog API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============== Models ==============

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    limit: int = 5

class ChatResponse(BaseModel):
    success: bool
    response: str
    books: List[Dict[str, Any]] = []
    response_time: float = 0
    session_id: str = ""

class UserRegister(BaseModel):
    username: str
    email: str
    password: str
    library_card: Optional[str] = None

class UserLogin(BaseModel):
    email: str
    password: str

class UserResponse(BaseModel):
    success: bool
    user_id: Optional[str] = None
    username: Optional[str] = None
    email: Optional[str] = None
    library_card: Optional[str] = None
    message: str = ""

# ============== Globals ==============

recommender = None
history_driver = None

# ============== Helpers ==============

def is_valid_query(message: str) -> bool:
    if len(message.strip()) < 3:
        return False

    # must contain letters
    if not any(c.isalpha() for c in message):
        return False

    # too long = likely garbage
    if len(message) > 300:
        return False

    return True

def hash_password(password: str) -> str:
    """Hash password with salt"""
    salt = "chatalog_salt_2024"
    return hashlib.sha256(f"{password}{salt}".encode()).hexdigest()

def get_recommender():
    global recommender
    if recommender is None:
        print("Initializing GraphRAG + Qwen...", flush=True)
        from phi_and_qwen_angela.QwenGraph import GraphRAGQwen
        recommender = GraphRAGQwen(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE)
        print("Ready!\n", flush=True)
    return recommender

def get_history_driver():
    global history_driver
    if history_driver is None:
        history_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with history_driver.session(database=DATABASE) as session:
            session.run("CREATE CONSTRAINT chat_session_id IF NOT EXISTS FOR (c:ChatSession) REQUIRE c.session_id IS UNIQUE")
            session.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE")
            session.run("CREATE CONSTRAINT user_email IF NOT EXISTS FOR (u:User) REQUIRE u.email IS UNIQUE")
    return history_driver

def create_session(user_id: Optional[str] = None):
    session_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    with get_history_driver().session(database=DATABASE) as session:
        if user_id:
            print(f"Creating session for user: {user_id}", flush=True)
            # First verify user exists
            user_check = session.run("MATCH (u:User {user_id: $uid}) RETURN u.user_id", uid=user_id).single()
            if user_check:
                result = session.run("""
                    MATCH (u:User {user_id: $uid})
                    CREATE (c:ChatSession {session_id: $sid, title: 'New Chat', created_at: $now, updated_at: $now})
                    CREATE (u)-[:HAS_SESSION]->(c)
                    RETURN c.session_id AS session_id
                """, sid=session_id, now=now, uid=user_id).single()
                print(f"Session {session_id} linked to user {user_id}", flush=True)
            else:
                print(f"User {user_id} not found, creating anonymous session", flush=True)
                result = session.run("""
                    CREATE (c:ChatSession {session_id: $sid, title: 'New Chat', created_at: $now, updated_at: $now})
                    RETURN c.session_id AS session_id
                """, sid=session_id, now=now).single()
        else:
            print("Creating anonymous session (no user_id)", flush=True)
            result = session.run("""
                CREATE (c:ChatSession {session_id: $sid, title: 'New Chat', created_at: $now, updated_at: $now})
                RETURN c.session_id AS session_id
            """, sid=session_id, now=now).single()
        return result['session_id']

def save_message(session_id, role, content, books=None):
    now = datetime.utcnow().isoformat()
    book_titles = [b.get('title', '') for b in (books or []) if b.get('title')]
    with get_history_driver().session(database=DATABASE) as session:
        session.run("""
            MATCH (c:ChatSession {session_id: $sid})
            CREATE (m:ChatMessage {message_id: $mid, role: $role, content: $content, books: $books, timestamp: $now})
            CREATE (c)-[:HAS_MESSAGE]->(m)
            SET c.updated_at = $now
            WITH c, m
            OPTIONAL MATCH (c)-[:HAS_MESSAGE]->(existing:ChatMessage)
            WITH c, m, count(existing) AS cnt
            WHERE cnt = 1 AND m.role = 'user'
            SET c.title = left(m.content, 50) + CASE WHEN size(m.content) > 50 THEN '...' ELSE '' END
        """, sid=session_id, mid=str(uuid.uuid4()), role=role, content=content, books=book_titles, now=now)

# ============== User Endpoints ==============

@app.post("/api/auth/register", response_model=UserResponse)
async def register(user: UserRegister):
    """Register a new user"""
    print(f"Registration attempt: {user.email}", flush=True)
    
    if len(user.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    user_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    password_hash = hash_password(user.password)
    
    with get_history_driver().session(database=DATABASE) as session:
        existing = session.run(
            "MATCH (u:User {email: $email}) RETURN u", 
            email=user.email.lower()
        ).single()
        
        if existing:
            raise HTTPException(status_code=400, detail="Email already registered")
        
        result = session.run("""
            CREATE (u:User {
                user_id: $uid,
                username: $username,
                email: $email,
                password_hash: $password_hash,
                library_card: $library_card,
                created_at: $now
            })
            RETURN u.user_id AS user_id, u.username AS username, u.email AS email, u.library_card AS library_card
        """, uid=user_id, username=user.username, email=user.email.lower(), 
            password_hash=password_hash, library_card=user.library_card or "", now=now).single()
        
        print(f"User created: {result['user_id']} ({result['email']})", flush=True)
        
        return UserResponse(
            success=True,
            user_id=result['user_id'],
            username=result['username'],
            email=result['email'],
            library_card=result['library_card'],
            message="Registration successful"
        )

@app.post("/api/auth/login", response_model=UserResponse)
async def login(credentials: UserLogin):
    """Login user"""
    print(f"Login attempt: {credentials.email}", flush=True)
    password_hash = hash_password(credentials.password)
    
    with get_history_driver().session(database=DATABASE) as session:
        result = session.run("""
            MATCH (u:User {email: $email, password_hash: $password_hash})
            RETURN u.user_id AS user_id, u.username AS username, u.email AS email, u.library_card AS library_card
        """, email=credentials.email.lower(), password_hash=password_hash).single()
        
        if not result:
            print(f"Login failed for: {credentials.email}", flush=True)
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        print(f"Login success: {result['user_id']} ({result['email']})", flush=True)
        
        return UserResponse(
            success=True,
            user_id=result['user_id'],
            username=result['username'],
            email=result['email'],
            library_card=result['library_card'],
            message="Login successful"
        )

@app.get("/api/auth/user/{user_id}", response_model=UserResponse)
async def get_user(user_id: str):
    """Get user profile"""
    with get_history_driver().session(database=DATABASE) as session:
        result = session.run("""
            MATCH (u:User {user_id: $uid})
            RETURN u.user_id AS user_id, u.username AS username, u.email AS email, u.library_card AS library_card
        """, uid=user_id).single()
        
        if not result:
            raise HTTPException(status_code=404, detail="User not found")
        
        return UserResponse(
            success=True,
            user_id=result['user_id'],
            username=result['username'],
            email=result['email'],
            library_card=result['library_card']
        )

# ============== User Sessions ==============

@app.get("/api/sessions/user/{user_id}")
async def get_user_sessions(user_id: str, limit: int = 20):
    """Get chat sessions for a specific user"""
    with get_history_driver().session(database=DATABASE) as session:
        return session.run("""
            MATCH (u:User {user_id: $uid})-[:HAS_SESSION]->(c:ChatSession)
            WHERE EXISTS { (c)-[:HAS_MESSAGE]->() }
            RETURN c.session_id AS session_id, c.title AS title, c.created_at AS created_at, c.updated_at AS updated_at
            ORDER BY c.updated_at DESC LIMIT $limit
        """, uid=user_id, limit=limit).data()

# ============== Chat Endpoints ==============

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    if not is_valid_query(request.message):
        session_id = request.session_id or create_session(request.user_id)
        return ChatResponse(
            success=True,
            response="I’m not sure I understood that. Could you rephrase your request for book recommendations?",
            books=[],
            session_id=session_id
        )
    
    print(f"\nQuery: {request.message}", flush=True)
    print(f"User ID: {request.user_id or 'anonymous'}", flush=True)
    print(f"Session ID: {request.session_id or 'new session'}", flush=True)
    
    try:
        session_id = request.session_id or create_session(request.user_id)
        save_message(session_id, "user", request.message)
        
        # Fetch conversation history to give the LLM context
        conversation_history = []
        with get_history_driver().session(database=DATABASE) as db:
            history = db.run("""
                MATCH (c:ChatSession {session_id: $sid})-[:HAS_MESSAGE]->(m:ChatMessage)
                RETURN m.role AS role, m.content AS content
                ORDER BY m.timestamp ASC
            """, sid=session_id).data()
            conversation_history = [{"role": h["role"], "content": h["content"]} for h in history]
        
        start = time.time()
        result = get_recommender().recommend(
            query=request.message,
            retrieval_method="smart",
            limit=request.limit,
            conversation_history=conversation_history
        )
        
        print(f"Found {len(result['retrieved_books'])} books in {round(time.time() - start, 2)}s", flush=True)

        # Handle empty results better
        if not result.get('retrieved_books'):
            return ChatResponse(
                success=True,
                response=f"I couldn't find exact matches for '{request.message}', but try simplifying your request or changing keywords.",
                books=[],
                session_id=session_id
            )
        
        save_message(session_id, "assistant", result['response'], books=result['retrieved_books'])
        # Link session to recommended books and user to genres
        if result['retrieved_books']:
            book_titles = [b['title'] for b in result['retrieved_books'] if 'title' in b]
            with get_history_driver().session(database=DATABASE) as db:
                db.run("""
                    MATCH (c:ChatSession {session_id: $sid})
                    UNWIND $titles AS title
                    MATCH (b:Book {title: title})
                    MERGE (c)-[:RECOMMENDED]->(b)
                """, sid=session_id, titles=book_titles)
                user_id = request.user_id
                if user_id:
                    db.run("""
                        MATCH (u:User {user_id: $uid})
                        MATCH (c:ChatSession {session_id: $sid})-[:RECOMMENDED]->(b:Book)-[:HAS_SUBJECT]->(g:Genre)
                        MERGE (u)-[:INTERESTED_IN]->(g)
                    """, uid=user_id, sid=session_id)
        return ChatResponse(
            success=True,
            response=result['response'],
            books=result['retrieved_books'],
            response_time=round(time.time() - start, 2),
            session_id=session_id
        )
    except Exception as e:
        print(f"Error: {e}", flush=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sessions")
async def get_sessions(limit: int = 20):
    with get_history_driver().session(database=DATABASE) as session:
        return session.run("""
            MATCH (c:ChatSession) WHERE EXISTS { (c)-[:HAS_MESSAGE]->() }
            RETURN c.session_id AS session_id, c.title AS title, c.created_at AS created_at, c.updated_at AS updated_at
            ORDER BY c.updated_at DESC LIMIT $limit
        """, limit=limit).data()

@app.get("/api/sessions/{session_id}/messages")
async def get_session_messages(session_id: str):
    with get_history_driver().session(database=DATABASE) as session:
        return session.run("""
            MATCH (c:ChatSession {session_id: $sid})-[:HAS_MESSAGE]->(m:ChatMessage)
            RETURN m.role AS role, m.content AS content, m.books AS books, m.timestamp AS timestamp
            ORDER BY m.timestamp ASC
        """, sid=session_id).data()

@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    with get_history_driver().session(database=DATABASE) as session:
        session.run("MATCH (c:ChatSession {session_id: $sid}) OPTIONAL MATCH (c)-[:HAS_MESSAGE]->(m) DETACH DELETE c, m", sid=session_id)
    return {"success": True}

app.mount("/static", StaticFiles(directory="frontend_taqwaa"), name="static")

@app.get("/")
async def home():
    return FileResponse("frontend_taqwaa/home.html")

@app.get("/{filename}.html")
async def pages(filename: str):
    return FileResponse(f"frontend_taqwaa/{filename}.html")

@app.get("/{filename}.css")
async def css(filename: str):
    return FileResponse(f"frontend_taqwaa/{filename}.css")

@app.get("/{filename}.png")
async def images(filename: str):
    return FileResponse(f"frontend_taqwaa/{filename}.png")

@app.get("/{filename}.js")
async def js(filename: str):
    return FileResponse(f"frontend_taqwaa/{filename}.js")

@app.get("/api/neo4j-config")
async def get_neo4j_config():
    """Return Neo4j connection info for frontend graph visualization"""
    return {
        "uri": NEO4J_URI.replace("neo4j+s://", "bolt+s://"),  # Neovis needs bolt protocol
        "user": NEO4J_USER,
        "password": NEO4J_PASSWORD,
        "database": DATABASE
    }

@app.get("/api/graph/inspect")
async def inspect_node_properties():
    """Return the actual property keys on each node type — use this to debug 'Unknown' labels."""
    results = {}
    with get_history_driver().session(database=DATABASE) as session:
        for label in ["Author", "Publisher", "Subject", "Book"]:
            record = session.run(
                f"MATCH (n:{label}) RETURN keys(n) AS props, n AS node LIMIT 1"
            ).single()
            if record:
                results[label] = {
                    "properties": record["props"],
                    "sample_values": dict(record["node"])
                }
    return results

@app.get("/api/graph")
async def get_graph_data(limit: int = 100):
    """Fetch graph data for visualization.

    Book-first approach: pick `limit` books, then collect ALL their
    relationships in one query.  This guarantees:
      - Every node in the response has a real name (no 'Unknown')
      - Every edge's endpoints exist in the node list
      - The graph stays manageable (limit books → bounded total nodes)
    """
    cypher = """
        MATCH (b:Book)
        WITH b LIMIT $limit
        MATCH (b)-[r]->(connected)
        WHERE connected:Author OR connected:Genre
           OR connected:Publisher OR connected:Location
        RETURN
            elementId(b)         AS source_id,
            'Book'               AS source_label,
            COALESCE(b.title, b.name, '') AS source_name,
            elementId(connected) AS target_id,
            labels(connected)[0] AS target_label,
            COALESCE(connected.name, connected.author, connected.publisher, connected.subject_1, connected.title, '') AS target_name,
            COALESCE(connected.birth_date, '') AS birth_date,
            type(r)              AS rel_type
    """

    nodes_map: dict = {}
    relationships: list = []

    with get_history_driver().session(database=DATABASE) as session:
        for record in session.run(cypher, limit=limit):
            sid, tid = record["source_id"], record["target_id"]
            sname = record["source_name"] or "Untitled"
            tname = record["target_name"] or record["target_label"]  # fall back to type, never blank

            if sid not in nodes_map:
                nodes_map[sid] = {"id": sid, "label": record["source_label"], "name": sname}
            if tid not in nodes_map:
                birth_date = record["birth_date"] or ""
                nodes_map[tid] = {"id": tid, "label": record["target_label"], "name": tname, "birth_date": birth_date}

            relationships.append({"source": sid, "target": tid, "type": record["rel_type"]})

    return {"nodes": list(nodes_map.values()), "relationships": relationships}
