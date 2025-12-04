"""
Chatalog FastAPI Backend
Run with: uvicorn api:app --reload
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Dict, Any
import time

# Import credentials from config
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE

# Create app
app = FastAPI(title="Chatalog API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request/Response models
class ChatRequest(BaseModel):
    message: str
    limit: int = 5

class ChatResponse(BaseModel):
    success: bool
    response: str
    books: List[Dict[str, Any]] = []
    response_time: float = 0

# Store recommender instance
recommender = None

def get_recommender():
    global recommender
    if recommender is None:
        from graphrag_tinyllama import GraphRAGTinyLlama
        recommender = GraphRAGTinyLlama(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE)
    return recommender

# API Endpoints
@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    
    try:
        start = time.time()
        result = get_recommender().recommend(
            query=request.message,
            retrieval_method="smart",
            limit=request.limit
        )
        
        return ChatResponse(
            success=True,
            response=result['response'],
            books=result['retrieved_books'],
            response_time=round(time.time() - start, 2)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Serve static files
app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/")
async def home():
    return FileResponse("frontend/home.html")

@app.get("/{filename}.html")
async def pages(filename: str):
    return FileResponse(f"frontend/{filename}.html")

@app.get("/{filename}.css")
async def css(filename: str):
    return FileResponse(f"frontend/{filename}.css")

@app.get("/{filename}.png")
async def images(filename: str):
    return FileResponse(f"frontend/{filename}.png")

@app.get("/{filename}.js")
async def js(filename: str):
    return FileResponse(f"frontend/{filename}.js")