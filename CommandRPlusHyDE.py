# CommandRPlusHyDE.py
import re
import numpy as np
from typing import List, Dict, Any, Tuple, Set
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from cohere import Client
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE, COHERE_API_KEY, COHERE_MODEL

co = Client(COHERE_API_KEY)

class CommandRPlusHyDE:
    STOP_WORDS: Set[str] = set([
        # many same stops as graph; keep concise
        'the','a','an','of','in','to','for','and','or','by','with','is','it','as','at','on','are','was','were',
        'books','book','library','have','want','need','find','looking','search'
    ])

    def __init__(self, neo4j_uri=NEO4J_URI, neo4j_user=NEO4J_USER, neo4j_password=NEO4J_PASSWORD, database=DATABASE):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.database = database
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

    def close(self):
        self.driver.close()

    def generate_hypothetical_document(self, query: str) -> str:
        prompt = f"Describe in 2-3 sentences what kind of books match: \"{query}\""
        try:
            resp = co.chat(model=COHERE_MODEL, message=prompt)
            return resp.text.strip()
        except:
            return query

    def _cosine(self, v1, v2):
        return float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2) + 1e-12))

    def retrieve_books_by_hyde(self, query: str, limit: int = 5) -> List[Dict[str,Any]]:
        hyp = self.generate_hypothetical_document(query)
        qemb = self.embedding_model.encode(hyp)
        # narrow candidate set with CONTAINS similar to Graph
        keywords = [w for w in query.lower().split() if len(w)>2 and w not in self.STOP_WORDS]
        if not keywords:
            keywords = query.lower().split()[:3]
        with self.driver.session(database=self.database) as session:
            raw = session.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WITH b, collect(DISTINCT a.author) AS authors, collect(DISTINCT g.subject_1) AS genres, toLower(b.title) AS t
                WHERE ANY(k IN $keywords WHERE t CONTAINS k)
                RETURN b.title AS title, authors, genres LIMIT 500
            """, keywords=keywords).data()
        candidates = []
        for r in raw:
            text = r['title'] or ''
            if r.get('authors'): text += " by " + ", ".join([a for a in r['authors'] if a])
            if r.get('genres'): text += " genres: " + ", ".join([g for g in r['genres'] if g][:3])
            emb = self.embedding_model.encode(text)
            score = self._cosine(qemb, emb)
            candidates.append({'title': r['title'], 'authors':[a for a in r.get('authors',[]) if a], 'genres':[g for g in r.get('genres',[]) if g], 'similarity_score': score})
        candidates.sort(key=lambda x: x['similarity_score'], reverse=True)
        return candidates[:limit]

    def format_context(self, books):
        if not books: return "No books found."
        return "\n".join([f"{i+1}. {b['title']}" for i,b in enumerate(books)])

    def generate_response(self, query, context, books=None):
        if not books: return "Hi! I'm Chatalog. I couldn't find any books matching your request."
        bl = "\n".join([f"{i+1}. {b['title']} by {', '.join(b.get('authors',[]))}" for i,b in enumerate(books)])
        prompt = f"You are Chatalog. A patron asked: \"{query}\" I found these books:\n{bl}\nReply concisely."
        try:
            resp = co.generate(model=COHERE_MODEL, prompt=prompt, max_tokens=200, temperature=0.5)
            return resp.generations[0].text.strip()
        except Exception as e:
            return f"[LLM ERROR] {e}"
