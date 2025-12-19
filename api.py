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

from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE

app = FastAPI(title="Chatalog API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    limit: int = 5

class ChatResponse(BaseModel):
    success: bool
    response: str
    books: List[Dict[str, Any]] = []
    response_time: float = 0
    session_id: str = ""

recommender = None
history_driver = None

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
    return history_driver

def create_session():
    session_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    with get_history_driver().session(database=DATABASE) as session:
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

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    
    print(f"\nQuery: {request.message}", flush=True)
    
    try:
        session_id = request.session_id or create_session()
        save_message(session_id, "user", request.message)
        
        start = time.time()
        result = get_recommender().recommend(
            query=request.message,
            retrieval_method="smart",
            limit=request.limit
        )
        
        print(f"Found {len(result['retrieved_books'])} books in {round(time.time() - start, 2)}s", flush=True)
        
        save_message(session_id, "assistant", result['response'], books=result['retrieved_books'])
        
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