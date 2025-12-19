import re
from typing import List, Dict, Any, Set, Tuple
from neo4j import GraphDatabase
import ollama

class SubGraphRAGPhi3:
    
    STOP_WORDS: Set[str] = {
        # Common words
        'the', 'a', 'an', 'of', 'in', 'to', 'for', 'and', 'or', 'by', 
        'with', 'is', 'it', 'as', 'at', 'on', 'are', 'was', 'were',
        # Pronouns
        'i', 'me', 'my', 'we', 'our', 'you', 'your', 'they', 'them',
        # Library-specific phrases
        'books', 'book', 'does', 'library', 'have', 'catalog', 'catalogue',
        'seattle', 'public', 'spl',
        # Request words
        'want', 'need', 'find', 'looking', 'search', 'show', 'give', 'get',
        'some', 'about', 'from', 'that', 'this', 'what', 'which', 'who',
        'can', 'could', 'would', 'should', 'will', 'may', 'might',
        'any', 'all', 'more', 'most', 'other', 'new', 'good', 'great',
        'read', 'something', 'anything', 'recommend', 'similar', 'like',
        'please', 'help', 'tell', 'list'
    }
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str, database: str = "neo4j"):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.database = database
        self.model_name = "phi3:mini"
        self._verify_connection()
        self._verify_ollama()
    
    def _verify_connection(self):
        try:
            with self.driver.session(database=self.database) as session:
                session.run("RETURN 1 AS test").single()
        except Exception as e:
            raise Exception(f"Neo4j connection failed: {e}")
    
    def _verify_ollama(self):
        try:
            models = ollama.list()
            if hasattr(models, 'models'):
                model_list = models.models
            elif isinstance(models, dict) and 'models' in models:
                model_list = models['models']
            else:
                model_list = models if isinstance(models, list) else []
            
            model_names = []
            for m in model_list:
                if isinstance(m, dict) and 'name' in m:
                    model_names.append(m['name'])
                elif hasattr(m, 'model'):
                    model_names.append(m.model)
                elif hasattr(m, 'name'):
                    model_names.append(m.name)
            
            if not any('phi3:mini' in name.lower() for name in model_names):
                raise RuntimeError("Phi 3 not found. Run: ollama pull phi3:mini")
        except Exception as e:
            raise Exception(f"Ollama check failed: {e}")
    
    def close(self):
        self.driver.close()
    
    def _parse_query_logic(self, query: str) -> tuple:
        """Parse AND/OR logic from query."""
        query_upper = query.upper()
        
        if " AND " in query_upper:
            parts = re.split(r'\s+AND\s+', query, flags=re.IGNORECASE)
            keywords = [p.strip().lower() for p in parts if p.strip()]
            return keywords, "AND"
        elif " OR " in query_upper:
            parts = re.split(r'\s+OR\s+', query, flags=re.IGNORECASE)
            keywords = [p.strip().lower() for p in parts if p.strip()]
            return keywords, "OR"
        else:
            keywords = self._filter_keywords(query)
            return keywords, "OR"
    
    def _parse_query_intent(self, query: str) -> Tuple[str, str]:
        query_lower = query.lower().strip()
        
        similar_patterns = [
            r'(?:books?\s+)?(?:like|similar\s+to|such\s+as)\s+(.+)',
            r'(?:recommend\s+)?(?:something|books?)\s+like\s+(.+)',
        ]
        for pattern in similar_patterns:
            match = re.search(pattern, query_lower)
            if match:
                return ("similar", match.group(1).strip())
        
        author_patterns = [
            r'(?:books?\s+)?(?:written\s+by|by)\s+(.+)',
            r'(.+?)(?:\'s)?\s+books?$',
        ]
        for pattern in author_patterns:
            match = re.search(pattern, query_lower)
            if match:
                term = match.group(1).strip()
                book_words = ['harry', 'potter', 'hunger', 'games', 'narnia']
                if not any(word in term for word in book_words):
                    return ("author", term)
        
        return ("keyword", query)
    
    def _filter_keywords(self, query: str) -> List[str]:
        words = query.lower().split()
        keywords = [w for w in words if w not in self.STOP_WORDS and len(w) > 2]
        if not keywords:
            keywords = [w for w in words if len(w) > 2][:3]
        keywords = keywords if keywords else words[:3]
        
        # Expand with singular/plural variants
        expanded = []
        for kw in keywords:
            expanded.append(kw)
            if kw.endswith('s') and len(kw) > 3:
                expanded.append(kw[:-1])
            elif not kw.endswith('s'):
                expanded.append(kw + 's')
        
        return list(dict.fromkeys(expanded))
    
    def find_seed_books(self, query: str, max_seeds: int = 3) -> List[Dict[str, Any]]:
        """Find seed books matching the query with AND/OR logic."""
        keywords, logic = self._parse_query_logic(query)
        
        if not keywords:
            return []
        
        with self.driver.session(database=self.database) as session:
            # Stage 1: Cypher CONTAINS to narrow down
            result = session.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WITH b, 
                     collect(DISTINCT a.author) AS authors,
                     collect(DISTINCT g.subject_1) AS genres,
                     toLower(b.title) AS title_lower,
                     [gen IN collect(DISTINCT g.subject_1) | toLower(coalesce(gen, ''))] AS genres_lower
                WHERE ANY(keyword IN $keywords WHERE 
                    title_lower CONTAINS keyword OR
                    ANY(g IN genres_lower WHERE g CONTAINS keyword)
                )
                RETURN 
                    b.title AS title,
                    authors,
                    genres
                LIMIT 200
            """, keywords=keywords).data()
            
            # Stage 2: Python word boundary filtering
            seeds = []
            for record in result:
                title = record['title'] or ''
                authors = [a for a in record['authors'] if a]
                genres = [g for g in record['genres'] if g]
                
                search_text = title.lower()
                for g in genres:
                    search_text += ' ' + g.lower()
                
                matches = 0
                for kw in keywords:
                    pattern = r'\b' + re.escape(kw) + r'\b'
                    if re.search(pattern, search_text):
                        matches += 1
                
                if logic == "AND" and matches < len(keywords):
                    continue
                if logic == "OR" and matches == 0:
                    continue
                
                seeds.append({
                    'title': title,
                    'author': authors[0] if authors else '',
                    'genres': genres,
                    'score': matches
                })
            
            seeds.sort(key=lambda x: x['score'], reverse=True)
            return seeds[:max_seeds]
    
    def extract_subgraph(self, seed_titles: List[str], max_nodes: int = 20) -> List[Dict[str, Any]]:
        """Extract books connected to seeds via graph relationships."""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (seed:Book)
                WHERE seed.title IN $seed_titles
                
                MATCH (seed)-[*1..2]-(related:Book)
                WHERE seed <> related
                
                WITH DISTINCT related
                
                OPTIONAL MATCH (related)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (related)-[:HAS_SUBJECT]->(g:Genre)
                
                OPTIONAL MATCH (seed:Book)-[:WRITTEN_BY]->(shared_a:Author)<-[:WRITTEN_BY]-(related)
                WHERE seed.title IN $seed_titles
                OPTIONAL MATCH (seed:Book)-[:HAS_SUBJECT]->(shared_g:Genre)<-[:HAS_SUBJECT]-(related)
                WHERE seed.title IN $seed_titles
                
                WITH related,
                     collect(DISTINCT a.author) AS authors,
                     collect(DISTINCT g.subject_1) AS genres,
                     count(DISTINCT shared_a) + count(DISTINCT shared_g) AS connections
                
                ORDER BY connections DESC
                LIMIT $max_nodes
                
                RETURN 
                    related.title AS title,
                    authors,
                    genres,
                    connections
            """, seed_titles=seed_titles, max_nodes=max_nodes).data()
            
            return [{
                'title': r['title'],
                'authors': [a for a in r['authors'] if a],
                'genres': [g for g in r['genres'] if g],
                'connection_strength': r['connections']
            } for r in result]
    
    def retrieve_books_by_subgraph(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Core method: Subgraph extraction from seed books."""
        seeds = self.find_seed_books(query, max_seeds=3)
        
        if not seeds:
            return self._keyword_search(query, limit)
        
        seed_titles = [s['title'] for s in seeds]
        subgraph = self.extract_subgraph(seed_titles, max_nodes=20)
        
        if not subgraph:
            return [{
                'title': s['title'],
                'authors': [s['author']] if s.get('author') else [],
                'genres': s.get('genres', []),
                'connection_strength': s.get('score', 0)
            } for s in seeds[:limit]]
        
        return subgraph[:limit]
    
    def _keyword_search(self, query: str, limit: int) -> List[Dict[str, Any]]:
        keywords = self._filter_keywords(query)
        
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WHERE ANY(keyword IN $keywords WHERE 
                    toLower(b.title) CONTAINS keyword OR
                    toLower(coalesce(g.subject_1, '')) CONTAINS keyword
                )
                WITH b, 
                     collect(DISTINCT a.author) AS authors,
                     collect(DISTINCT g.subject_1) AS genres
                LIMIT $limit
                RETURN 
                    b.title AS title,
                    authors,
                    genres
            """, keywords=keywords, limit=limit).data()
            
            return [{
                'title': r['title'],
                'authors': [a for a in r['authors'] if a],
                'genres': [g for g in r['genres'] if g],
                'connection_strength': 0
            } for r in result]
    
    def retrieve_books_by_author(self, author_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        cleaned = author_name.lower().replace('.', ' ').replace(',', ' ')
        words = [p.strip() for p in cleaned.split() if len(p.strip()) > 0]
        
        name_parts = []
        for word in words:
            if len(word) <= 3 and word.isalpha():
                name_parts.extend(list(word))
            else:
                name_parts.append(word)
        
        if not name_parts:
            name_parts = [author_name.lower().strip()]
        
        surname = max(name_parts, key=len)
        
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (b:Book)-[:WRITTEN_BY]->(a:Author)
                WHERE toLower(a.author) CONTAINS $surname
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                RETURN DISTINCT
                    b.title AS title,
                    a.author AS author,
                    collect(DISTINCT g.subject_1) AS genres
                LIMIT 50
            """, surname=surname)
            
            books = []
            for record in result:
                author_full = record['author'] or ''
                author_lower = author_full.lower()
                
                all_match = True
                for part in name_parts:
                    if len(part) == 1:
                        pattern = r'(^|[\s,])' + part + r'($|[\s.])'
                        if not re.search(pattern, author_lower):
                            all_match = False
                            break
                    else:
                        if part not in author_lower:
                            all_match = False
                            break
                
                if all_match:
                    books.append({
                        'title': record['title'],
                        'authors': [author_full] if author_full else [],
                        'genres': [g for g in record['genres'] if g],
                        'connection_strength': 0
                    })
                    if len(books) >= limit:
                        break
            
            return books
    
    def smart_retrieve(self, query: str, limit: int = 5) -> Tuple[List[Dict[str, Any]], str]:
        intent, term = self._parse_query_intent(query)
        
        if intent == "author":
            books = self.retrieve_books_by_author(term, limit)
            return books, "author"
        else:
            books = self.retrieve_books_by_subgraph(query, limit)
            return books, "subgraph"
    
    def format_context(self, books: List[Dict[str, Any]]) -> str:
        if not books:
            return "No books found."
        
        lines = ["Available books:\n"]
        for i, book in enumerate(books, 1):
            lines.append(f"{i}. {book['title']}")
            if book.get('authors'):
                lines.append(f"   Authors: {', '.join(book['authors'])}")
            if book.get('genres'):
                lines.append(f"   Genres: {', '.join(book['genres'][:3])}")
        
        return "\n".join(lines)
    
    def generate_response(self, query: str, context: str, books: List[Dict[str, Any]] = None, stream: bool = False) -> str:
        if not books:
            return """Hi! I'm Chatalog, a chatbot for the Seattle Public Library.

I couldn't find any books matching your request in our catalog.

You can suggest a title for the library to add here:
https://www.spl.org/books-and-media/suggest-a-title

Or try a different search - maybe with different keywords?"""
        
        num_catalog_books = len(books)
        book_list = ""
        for i, book in enumerate(books, 1):
            book_list += f"\n{i}. {book.get('title', 'Unknown')}"
            if book.get('authors'):
                book_list += f" by {', '.join(book['authors'])}"
            if book.get('genres'):
                book_list += f" (Genres: {', '.join(book['genres'][:2])})"
        
        prompt = f"""You are Chatalog, a friendly chatbot for the Seattle Public Library.

A patron asked: "{query}"

I found {num_catalog_books} books in our catalog:
{book_list}

Write a response that:
1. Starts with: "Hi! I'm Chatalog, a chatbot for the Seattle Public Library."
2. Says: "I found {num_catalog_books} books in our catalog that match your request:"
3. Lists each catalog book with title, author, and a one-sentence description
4. Then says: "These books are not in our catalog, but you might also enjoy:"
5. Suggests 3-5 additional books NOT listed above that fit the patron's interest
6. Ends with: "Want any of these titles added to our collection? Request them here: https://www.spl.org/books-and-media/suggest-a-title"

Keep it friendly and helpful. Do not make up information about the catalog books."""

        try:
            if stream:
                response_parts = []
                for chunk in ollama.generate(model=self.model_name, prompt=prompt, stream=True, options={"temperature": 0.7}):
                    text = chunk.get('response', '')
                    print(text, end='', flush=True)
                    response_parts.append(text)
                print()
                return ''.join(response_parts)
            else:
                response = ollama.generate(model=self.model_name, prompt=prompt, options={"temperature": 0.7})
                return response['response']
        except Exception as e:
            return f"Error generating response: {e}"
    
    def recommend(self, query: str, retrieval_method: str = "smart", limit: int = 5, stream: bool = False) -> Dict[str, Any]:
        print(f"\nQuery: {query}")
        print(f"Method: SubGraph RAG ({retrieval_method})\n")
        
        if retrieval_method == "smart":
            books, method_used = self.smart_retrieve(query, limit)
        else:
            books = self.retrieve_books_by_subgraph(query, limit)
            method_used = "subgraph"
        
        print(f"Found {len(books)} books (method: {method_used})\n")
        
        context = self.format_context(books)
        response = self.generate_response(query, context, books=books, stream=stream)
        
        return {
            'query': query,
            'retrieval_method': method_used,
            'retrieved_books': books,
            'num_books_retrieved': len(books),
            'response': response
        }


def main():
    from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE
    
    system = SubGraphRAGPhi3(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE)
    
    try:
        print("\nChatalog - Seattle Public Library Assistant")
        print("Powered by SubGraph RAG\n")
        print("Type 'quit' to exit\n")
        
        while True:
            query = input("What are you looking for? ")
            
            if query.lower() in ['quit', 'exit', 'q']:
                print("\nThanks for visiting Chatalog!\n")
                break
            
            if not query.strip():
                continue
            
            result = system.recommend(query, retrieval_method="smart", limit=5, stream=True)
            print("\n")
    finally:
        system.close()


if __name__ == "__main__":
    main()
