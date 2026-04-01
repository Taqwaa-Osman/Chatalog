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
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    
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

@app.get("/api/graph")
async def get_graph_data(limit: int = 500):
    """Fetch graph data for visualization.

    Fetches each relationship type with its own sub-limit so no single
    type (e.g. WRITTEN_BY) can starve Subject / Publisher nodes out of
    the response.  All relationship endpoints are guaranteed to exist
    in the returned nodes list.
    """
    per_type = max(50, limit // 4)

    # Single UNION ALL query — one round-trip, each rel type gets its own LIMIT
    # so no type can starve the others out of the result set.
    cypher = """
        MATCH (a:Book)-[r:WRITTEN_BY]->(b:Author)
        RETURN elementId(a) AS source_id, labels(a)[0] AS source_label, COALESCE(a.title, a.name, 'Unknown') AS source_name,
               elementId(b) AS target_id, labels(b)[0] AS target_label, COALESCE(b.title, b.name, 'Unknown') AS target_name,
               type(r) AS rel_type
        LIMIT $lim
    UNION ALL
        MATCH (a:Book)-[r:HAS_SUBJECT]->(b:Subject)
        RETURN elementId(a) AS source_id, labels(a)[0] AS source_label, COALESCE(a.title, a.name, 'Unknown') AS source_name,
               elementId(b) AS target_id, labels(b)[0] AS target_label, COALESCE(b.title, b.name, 'Unknown') AS target_name,
               type(r) AS rel_type
        LIMIT $lim
    UNION ALL
        MATCH (a:Book)-[r:PUBLISHED_BY]->(b:Publisher)
        RETURN elementId(a) AS source_id, labels(a)[0] AS source_label, COALESCE(a.title, a.name, 'Unknown') AS source_name,
               elementId(b) AS target_id, labels(b)[0] AS target_label, COALESCE(b.title, b.name, 'Unknown') AS target_name,
               type(r) AS rel_type
        LIMIT $lim
    UNION ALL
        MATCH (a:Book)-[r:HAS_LOCATION]->(b:Location)
        RETURN elementId(a) AS source_id, labels(a)[0] AS source_label, COALESCE(a.title, a.name, 'Unknown') AS source_name,
               elementId(b) AS target_id, labels(b)[0] AS target_label, COALESCE(b.title, b.name, 'Unknown') AS target_name,
               type(r) AS rel_type
        LIMIT $lim
    """

    nodes_map: dict = {}
    relationships: list = []

    with get_history_driver().session(database=DATABASE) as session:
        for record in session.run(cypher, lim=per_type):
            sid, tid = record["source_id"], record["target_id"]
            if sid not in nodes_map:
                nodes_map[sid] = {"id": sid, "label": record["source_label"], "name": record["source_name"]}
            if tid not in nodes_map:
                nodes_map[tid] = {"id": tid, "label": record["target_label"], "name": record["target_name"]}
            relationships.append({"source": sid, "target": tid, "type": record["rel_type"]})

    return {
        "nodes": list(nodes_map.values()),
        "relationships": relationships
    }