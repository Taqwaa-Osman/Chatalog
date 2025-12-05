# CommandRPlusVector.py
import re
import numpy as np
from typing import List, Dict, Any, Tuple, Set
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from cohere import Client
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE, COHERE_API_KEY, COHERE_MODEL

co = Client(COHERE_API_KEY)

class CommandRPlusVector:
    STOP_WORDS = set(['the','a','an','of','in','to','for','and','or','by','books','book'])
    def __init__(self, neo4j_uri=NEO4J_URI, neo4j_user=NEO4J_USER, neo4j_password=NEO4J_PASSWORD, database=DATABASE):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.database = database
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.book_data=[]
        self.book_embeddings=None
        self._precompute()

    def _precompute(self, max_books=3000):
        with self.driver.session(database=self.database) as s:
            rows = s.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WITH b, collect(DISTINCT a.author) AS authors, collect(DISTINCT g.subject_1) AS genres
                RETURN b.title AS title, authors, genres LIMIT $max_books
            """, max_books=max_books).data()
        texts=[]
        for r in rows:
            text = r['title'] or ''
            if r.get('authors'): text += " by " + ", ".join([a for a in r['authors'] if a])
            if r.get('genres'): text += " genres: " + ", ".join([g for g in r['genres'] if g][:3])
            self.book_data.append({'title':r['title'],'authors':[a for a in r.get('authors',[]) if a],'genres':[g for g in r.get('genres',[]) if g],'text':text})
            texts.append(text)
        if texts:
            self.book_embeddings = self.embedding_model.encode(texts, show_progress_bar=False)

    def retrieve_books_by_vector(self, query, limit=5):
        if not self.book_embeddings:
            return []
        qemb = self.embedding_model.encode(query)
        sims=[]
        for i,be in enumerate(self.book_embeddings):
            sim = float(np.dot(qemb, be)/(np.linalg.norm(qemb)*np.linalg.norm(be)+1e-12))
            sims.append((i,sim))
        sims.sort(key=lambda x:x[1], reverse=True)
        out=[]
        for idx,score in sims[:limit]:
            b = dict(self.book_data[idx])
            b['similarity_score']=score
            out.append(b)
        return out

    def retrieve_books_by_author(self, author_name, limit=10):
        surname = max([p for p in re.split(r'[\s,.]+', author_name.lower()) if p], key=len)
        with self.driver.session(database=self.database) as s:
            res = s.run("""
                MATCH (b:Book)-[:WRITTEN_BY]->(a:Author)
                WHERE toLower(a.author) CONTAINS $surname
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                RETURN DISTINCT b.title AS title, a.author AS author, collect(DISTINCT g.subject_1) AS genres LIMIT $limit
            """, surname=surname, limit=limit)
            out=[]
            for r in res:
                out.append({'title':r['title'],'authors':[r['author']],'genres':[g for g in r['genres'] if g],'similarity_score':1.0})
            return out

    def format_context(self, books): 
        if not books: return "No books found."
        return "\n".join([f"{i+1}. {b['title']}" for i,b in enumerate(books)])

    def generate_response(self, query, context, books=None):
        if not books: return "Hi! I'm Chatalog. I couldn't find any books matching your request."
        bl = "\n".join([f"{i+1}. {b['title']} by {', '.join(b.get('authors',[]))}" for i,b in enumerate(books)])
        prompt = f"You are Chatalog. Query: \"{query}\" Found:\n{bl}\nReply concisely."
        try:
            resp = co.chat(model=COHERE_MODEL, message=prompt)
            return resp.generations[0].text.strip()
        except:
            return "Error generating answer"
