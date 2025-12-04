import re
import numpy as np
from collections import Counter
from typing import List, Dict, Any, Set, Tuple
from neo4j import GraphDatabase
import ollama
from sentence_transformers import SentenceTransformer


class MultiQueryRAGMinistral3:
    
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
        self.model_name = "ministral-3:3b"
        
        print("Loading embedding model...")
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        print("Embedding model loaded")
        
        # Try to load cross-encoder for reranking
        try:
            from sentence_transformers import CrossEncoder
            self.cross_encoder = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
            self.use_cross_encoder = True
            print("Cross-encoder loaded for reranking")
        except:
            self.cross_encoder = None
            self.use_cross_encoder = False
            print("Using frequency-based reranking")
        
        self.book_embeddings = None
        self.book_data = None
        
        self._verify_connection()
        self._verify_ollama()
        
        print("Pre-computing book embeddings...")
        self._precompute_embeddings()
        print(f"Embedded {len(self.book_data)} books")
    
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
            
            if not any('ministral-3:3b' in name.lower() for name in model_names):
                raise RuntimeError("ministral3 not found. Run: ollama pull ministral-3:3b")
        except Exception as e:
            raise Exception(f"Ollama check failed: {e}")
    
    def close(self):
        self.driver.close()
    
    def _precompute_embeddings(self, max_books: int = 5000):
        print(f"Loading up to {max_books} books for embedding...")
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WITH b, 
                     collect(DISTINCT a.author) AS authors,
                     collect(DISTINCT g.subject_1) AS genres
                RETURN 
                    b.title AS title,
                    authors,
                    genres
                LIMIT $max_books
            """, max_books=max_books).data()
        
        self.book_data = []
        texts_to_embed = []
        
        for record in result:
            text_parts = [record['title']]
            authors = [a for a in record.get('authors', []) if a]
            genres = [g for g in record.get('genres', []) if g]
            
            if authors:
                text_parts.append(f"by {', '.join(authors)}")
            if genres:
                text_parts.append(f"genres: {', '.join(genres[:3])}")
            
            book_text = " ".join(text_parts)
            texts_to_embed.append(book_text)
            
            self.book_data.append({
                'title': record['title'],
                'authors': authors,
                'genres': genres,
                'text': book_text
            })
        
        self.book_embeddings = self.embedding_model.encode(texts_to_embed, show_progress_bar=True)
    
    def _parse_query_logic(self, query: str) -> tuple:
        """Parse AND/OR logic from query with plural expansion."""
        query_upper = query.upper()
        
        if " AND " in query_upper:
            parts = re.split(r'\s+AND\s+', query, flags=re.IGNORECASE)
            keywords = [p.strip().lower() for p in parts if p.strip()]
            logic = "AND"
        elif " OR " in query_upper:
            parts = re.split(r'\s+OR\s+', query, flags=re.IGNORECASE)
            keywords = [p.strip().lower() for p in parts if p.strip()]
            logic = "OR"
        else:
            words = query.lower().split()
            keywords = [w for w in words if w not in self.STOP_WORDS and len(w) > 2]
            keywords = keywords if keywords else words[:3]
            logic = "OR"
        
        # Expand with singular/plural variants
        expanded = []
        for kw in keywords:
            expanded.append(kw)
            if kw.endswith('s') and len(kw) > 3:
                expanded.append(kw[:-1])
            elif not kw.endswith('s'):
                expanded.append(kw + 's')
        
        return list(dict.fromkeys(expanded)), logic
    
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
    
    def generate_query_variations(self, query: str, num_variations: int = 3) -> List[str]:
        """Generate multiple query variations using Ministral3."""
        prompt = f"""Generate {num_variations} different search queries for books based on: "{query}"
Output only the queries, one per line. Focus on different aspects like genre, theme, mood."""

        try:
            response = ollama.generate(model=self.model_name, prompt=prompt, options={"temperature": 0.8})
            lines = response['response'].strip().split('\n')
            variations = [line.strip().strip('-').strip('*').strip() for line in lines if line.strip()]
            variations = [v for v in variations if 3 < len(v) < 100]
            return [query] + variations[:num_variations]
        except:
            return [query]
    
    def _retrieve_single_query(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieve for a single query using vector similarity."""
        query_embedding = self.embedding_model.encode(query)
        
        similarities = []
        for i, book_emb in enumerate(self.book_embeddings):
            sim = float(np.dot(query_embedding, book_emb) / 
                       (np.linalg.norm(query_embedding) * np.linalg.norm(book_emb)))
            similarities.append((i, sim))
        
        similarities.sort(key=lambda x: x[1], reverse=True)
        
        results = []
        for idx, sim_score in similarities[:limit]:
            book = self.book_data[idx].copy()
            book['similarity_score'] = sim_score
            results.append(book)
        
        return results
    
    def rerank_by_frequency(self, all_results: List[List[Dict[str, Any]]], limit: int = 5) -> List[Dict[str, Any]]:
        """Rerank by frequency: books in multiple results rank higher."""
        title_counts = Counter()
        title_to_book = {}
        title_scores = {}
        
        for results in all_results:
            for book in results:
                title = book['title']
                title_counts[title] += 1
                title_to_book[title] = book
                if title not in title_scores:
                    title_scores[title] = []
                title_scores[title].append(book.get('similarity_score', 0))
        
        ranked_books = []
        for title, count in title_counts.items():
            book = title_to_book[title].copy()
            avg_similarity = sum(title_scores[title]) / len(title_scores[title])
            book['rerank_score'] = count * avg_similarity
            book['frequency'] = count
            ranked_books.append(book)
        
        ranked_books.sort(key=lambda x: x['rerank_score'], reverse=True)
        return ranked_books[:limit]
    
    def retrieve_books_by_multiquery(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Core method: Multi-query retrieval with reranking and AND/OR filtering."""
        keywords, logic = self._parse_query_logic(query)
        queries = self.generate_query_variations(query, num_variations=3)
        
        all_results = []
        for q in queries:
            results = self._retrieve_single_query(q, limit=10)
            all_results.append(results)
        
        # Combine unique results
        combined = []
        seen_titles = set()
        for results in all_results:
            for book in results:
                if book['title'] not in seen_titles:
                    # Apply AND filter with word boundaries
                    if logic == "AND":
                        text_lower = book['text'].lower()
                        if not all(re.search(r'\b' + re.escape(kw) + r'\b', text_lower) for kw in keywords):
                            continue
                    combined.append(book)
                    seen_titles.add(book['title'])
        
        # Rerank
        if self.use_cross_encoder and self.cross_encoder:
            pairs = [(query, book['text']) for book in combined]
            if pairs:
                scores = self.cross_encoder.predict(pairs)
                for i, book in enumerate(combined):
                    book['rerank_score'] = float(scores[i])
                combined.sort(key=lambda x: x['rerank_score'], reverse=True)
            return combined[:limit]
        else:
            return self.rerank_by_frequency(all_results, limit)
    
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
                        'similarity_score': 1.0
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
            books = self.retrieve_books_by_multiquery(query, limit)
            return books, "multiquery"
    
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
        print(f"Method: Multi-Query RAG ({retrieval_method})\n")
        
        if retrieval_method == "smart":
            books, method_used = self.smart_retrieve(query, limit)
        else:
            books = self.retrieve_books_by_multiquery(query, limit)
            method_used = "multiquery"
        
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
    
    system = MultiQueryRAGMinistral3(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE)
    
    try:
        print("\nChatalog - Seattle Public Library Assistant")
        print("Powered by Multi-Query RAG\n")
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
