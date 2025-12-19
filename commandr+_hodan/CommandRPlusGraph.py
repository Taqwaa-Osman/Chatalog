# CommandRPlusGraph.py
import re
from typing import List, Dict, Any, Set, Tuple
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from cohere import Client

from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE, COHERE_API_KEY, COHERE_MODEL

co = Client(COHERE_API_KEY)

class CommandRPlusGraph:
    STOP_WORDS: Set[str] = {
        'the','a','an','of','in','to','for','and','or','by','with','is','it','as','at','on','are','was','were',
        'i','me','my','we','our','you','your','they','them',
        'books','book','does','library','have','catalog','catalogue','seattle','public','spl',
        'want','need','find','looking','search','show','give','get','some','about','from','that','this','what','which','who',
        'can','could','would','should','will','may','might','any','all','more','most','other','new','good','great',
        'read','something','anything','recommend','similar','like','please','help','tell','list'
    }

    def __init__(self, neo4j_uri=NEO4J_URI, neo4j_user=NEO4J_USER, neo4j_password=NEO4J_PASSWORD, database=DATABASE):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.database = database
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

    def close(self):
        self.driver.close()

    def _filter_keywords(self, query: str) -> List[str]:
        words = query.lower().split()
        keywords = [w for w in words if w not in self.STOP_WORDS and len(w) > 2]
        if not keywords:
            keywords = [w for w in words if len(w) > 2][:3]
        expanded = []
        for kw in keywords:
            expanded.append(kw)
            if kw.endswith('s') and len(kw) > 3:
                expanded.append(kw[:-1])
            elif not kw.endswith('s'):
                expanded.append(kw + 's')
        return list(dict.fromkeys(expanded))

    def _parse_query_intent(self, query: str) -> Tuple[str,str]:
        q = query.lower().strip()
        m = re.search(r'(?:written by|by)\s+(.+)', q)
        if m:
            return "author", m.group(1).strip()
        m = re.search(r'(?:like|similar to|such as)\s+(.+)', q)
        if m:
            return "similar", m.group(1).strip()
        return "keyword", query

    def retrieve_books_by_keyword(self, query: str, limit: int = 10, debug: bool = False) -> List[Dict[str,Any]]:
        keywords = self._filter_keywords(query)
        if not keywords:
            return []
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WITH b, collect(DISTINCT a.author) AS authors, collect(DISTINCT g.subject_1) AS genres,
                     toLower(b.title) AS title_lower, [gen IN collect(DISTINCT g.subject_1) | toLower(coalesce(gen, ''))] AS genres_lower
                WHERE ANY(keyword IN $keywords WHERE title_lower CONTAINS keyword OR ANY(g IN genres_lower WHERE g CONTAINS keyword))
                RETURN b.title AS title, authors, genres
                LIMIT 500
            """, keywords=keywords).data()

        books = []
        for record in result:
            title = record['title'] or ''
            authors = [a for a in record['authors'] if a]
            genres = [g for g in record['genres'] if g]
            search_text = title.lower() + " " + " ".join([g.lower() for g in genres])
            matches = sum(1 for kw in keywords if re.search(r'\b' + re.escape(kw) + r'\b', search_text))
            if matches == 0:
                continue
            books.append({'title': title, 'authors': authors, 'genres': genres, 'relevance': matches})
        books.sort(key=lambda x: x['relevance'], reverse=True)
        return books[:limit]

    def retrieve_books_by_author(self, author_name: str, limit: int = 10) -> List[Dict[str,Any]]:
        if not author_name:
            return []
        surname = max([p for p in re.split(r'[\s,.]+', author_name.lower()) if p], key=len)
        with self.driver.session(database=self.database) as session:
            rec = session.run("""
                MATCH (b:Book)-[:WRITTEN_BY]->(a:Author)
                WHERE toLower(a.author) CONTAINS $surname
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                RETURN DISTINCT b.title AS title, a.author AS author, collect(DISTINCT g.subject_1) AS genres
                LIMIT $limit
            """, surname=surname, limit=limit)
            out = []
            for r in rec:
                out.append({'title': r['title'], 'authors':[r['author']] if r['author'] else [], 'genres':[g for g in r['genres'] if g]})
            return out

    def retrieve_similar_books(self, book_title: str, limit: int = 5) -> List[Dict[str,Any]]:
        # find source and walk graph
        keywords = self._filter_keywords(book_title)
        if not keywords:
            return []
        with self.driver.session(database=self.database) as session:
            src = session.run("""
                MATCH (b:Book)
                WHERE ANY(keyword IN $keywords WHERE toLower(b.title) CONTAINS keyword)
                RETURN b.title AS title LIMIT 50
            """, keywords=keywords).data()
            # best word-boundary match:
            source_title = None
            best = 0
            for r in src:
                t = (r['title'] or '').lower()
                c = sum(1 for kw in keywords if re.search(r'\b' + re.escape(kw) + r'\b', t))
                if c>best:
                    best=c; source_title=r['title']
            if not source_title:
                return []
            res = session.run("""
                MATCH (s:Book {title:$source_title})
                OPTIONAL MATCH (s)-[:HAS_SUBJECT]->(g:Genre)<-[:HAS_SUBJECT]-(similar:Book)
                OPTIONAL MATCH (s)-[:WRITTEN_BY]->(a:Author)<-[:WRITTEN_BY]-(same_author:Book)
                WITH collect(DISTINCT similar)+collect(DISTINCT same_author) AS related
                UNWIND related AS book
                OPTIONAL MATCH (book)-[:WRITTEN_BY]->(ba:Author)
                OPTIONAL MATCH (book)-[:HAS_SUBJECT]->(bg:Genre)
                RETURN DISTINCT book.title AS title, collect(DISTINCT ba.author) AS authors, collect(DISTINCT bg.subject_1) AS genres
                LIMIT $limit
            """, source_title=source_title, limit=limit).data()
            out=[]
            for r in res:
                out.append({'title': r['title'], 'authors':[a for a in r['authors'] if a], 'genres':[g for g in r['genres'] if g]})
            return out

    def format_context(self, books: List[Dict[str,Any]]) -> str:
        if not books:
            return "No books found."
        lines=[]
        for i,b in enumerate(books,1):
            lines.append(f"{i}. {b['title']}")
            if b.get('authors'): lines.append(f"   Authors: {', '.join(b['authors'])}")
            if b.get('genres'): lines.append(f"   Genres: {', '.join(b['genres'][:3])}")
        return "\n".join(lines)

    def generate_response(self, query: str, context: str, books: List[Dict[str,Any]]=None) -> str:
        """Use Cohere command-r-plus model to generate the user-facing answer."""
        if not books:
            return "Hi! I'm Chatalog. I couldn't find any books matching your request in our catalog."
        book_list = ""
        for i,b in enumerate(books,1):
            book_list += f"\n{i}. {b.get('title','Unknown')}"
            if b.get('authors'): book_list += f" by {', '.join(b.get('authors'))}"
        prompt = f"""You are Chatalog, a friendly chatbot for the Seattle Public Library.
A patron asked: "{query}"
I found {len(books)} books:
{book_list}
Provide a concise, helpful answer (no invented facts)."""
        try:
            resp = co.chat(model=COHERE_MODEL, message=prompt)
            return resp.text.strip()
        except Exception as e:
            return f"[LLM ERROR] {e}"

