# CommandRPlusSubGraph.py
import re
from typing import List, Dict, Any, Tuple, Set
from neo4j import GraphDatabase
from cohere import Client
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE, COHERE_API_KEY, COHERE_MODEL

co = Client(COHERE_API_KEY)

class CommandRPlusSubGraph:
    STOP_WORDS = set(['the','a','an','of','in','to','for','and','or','by','books','book'])
    def __init__(self, neo4j_uri=NEO4J_URI, neo4j_user=NEO4J_USER, neo4j_password=NEO4J_PASSWORD, database=DATABASE):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.database = database

    def close(self): self.driver.close()

    def _filter_keywords(self, query):
        words=[w for w in query.lower().split() if w not in self.STOP_WORDS and len(w)>2]
        if not words: words=query.lower().split()[:3]
        expanded=[]
        for kw in words:
            expanded.append(kw)
            if kw.endswith('s') and len(kw)>3: expanded.append(kw[:-1])
            else: expanded.append(kw+'s')
        return list(dict.fromkeys(expanded))

    def find_seed_books(self, query, max_seeds=3):
        keywords = self._filter_keywords(query)
        if not keywords: return []
        with self.driver.session(database=self.database) as session:
            rows = session.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WITH b, collect(DISTINCT a.author) AS authors, collect(DISTINCT g.subject_1) AS genres, toLower(b.title) AS t
                WHERE ANY(k IN $keywords WHERE t CONTAINS k OR ANY(s IN [gen IN collect(DISTINCT g.subject_1) | toLower(coalesce(gen,''))] WHERE s CONTAINS k))
                RETURN b.title AS title, authors, genres LIMIT 200
            """, keywords=keywords).data()
        seeds=[]
        for r in rows:
            title=r['title'] or ''
            authors=[a for a in r['authors'] if a]
            genres=[g for g in r['genres'] if g]
            search = title.lower() + " " + " ".join([g.lower() for g in genres])
            matches=sum(1 for kw in keywords if re.search(r'\b'+re.escape(kw)+r'\b', search))
            if matches==0: continue
            seeds.append({'title':title,'author':authors[0] if authors else '','genres':genres,'score':matches})
        seeds.sort(key=lambda x:x['score'], reverse=True)
        return seeds[:max_seeds]

    def extract_subgraph(self, seed_titles: List[str], max_nodes=20):
        if not seed_titles: return []
        with self.driver.session(database=self.database) as s:
            res = s.run("""
                MATCH (seed:Book) WHERE seed.title IN $seed_titles
                MATCH (seed)-[*1..2]-(related:Book) WHERE seed<>related
                WITH DISTINCT related
                OPTIONAL MATCH (related)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (related)-[:HAS_SUBJECT]->(g:Genre)
                RETURN related.title AS title, collect(DISTINCT a.author) AS authors, collect(DISTINCT g.subject_1) AS genres LIMIT $max_nodes
            """, seed_titles=seed_titles, max_nodes=max_nodes).data()
        out=[]
        for r in res:
            out.append({'title':r['title'],'authors':[a for a in r['authors'] if a],'genres':[g for g in r['genres'] if g],'connection_strength':0})
        return out

    def retrieve_books_by_subgraph(self, query, limit=5):
        seeds=self.find_seed_books(query, max_seeds=3)
        if not seeds:
            return self._keyword_search(query, limit)
        seed_titles=[s['title'] for s in seeds]
        sub = self.extract_subgraph(seed_titles, max_nodes=20)
        if not sub:
            return [{'title': s['title'], 'authors': [s['author']] if s.get('author') else [], 'genres': s.get('genres',[])} for s in seeds[:limit]]
        return sub[:limit]

    def _keyword_search(self, query, limit=5):
        keywords=self._filter_keywords(query)
        with self.driver.session(database=self.database) as s:
            res=s.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WHERE ANY(k IN $keywords WHERE toLower(b.title) CONTAINS k OR toLower(coalesce(g.subject_1,'')) CONTAINS k)
                WITH b, collect(DISTINCT a.author) AS authors, collect(DISTINCT g.subject_1) AS genres LIMIT $limit
                RETURN b.title AS title, authors, genres
            """, keywords=keywords, limit=limit).data()
        out=[]
        for r in res:
            out.append({'title':r['title'],'authors':[a for a in r['authors'] if a],'genres':[g for g in r['genres'] if g]})
        return out

    def format_context(self, books):
        if not books: return "No books found."
        return "\n".join([f"{i+1}. {b['title']}" for i,b in enumerate(books)])

    def generate_response(self, query, context, books=None):
        if not books: return "Hi! I'm Chatalog. I couldn't find any books matching your request."
        bl = "\n".join([f"{i+1}. {b['title']} by {', '.join(b.get('authors',[]))}" for i,b in enumerate(books)])
        prompt = f"You are Chatalog. User: \"{query}\" Found:\n{bl}\nReply concisely."
        try:
            resp = co.chat(model=COHERE_MODEL, message=prompt)
            return resp.text.strip()
        except:
            return "Error generating answer"
