import re
from typing import List, Dict, Any, Set, Tuple
from neo4j import GraphDatabase
import ollama


class GraphRAGQwen:

    STOP_WORDS: Set[str] = {
        # Common words
        'the', 'a', 'an', 'of', 'in', 'to', 'for', 'are', 'there', 'by',
        'with', 'is', 'it', 'as', 'at', 'on', 'was', 'were',
        # Pronouns
        'i', 'me', 'my', 'we', 'our', 'you', 'your', 'they', 'them',
        # Library-specific phrases
        'books', 'book', 'does', 'library', 'have', 'catalog', 'catalogue',
        'seattle', 'public', 'spl', 'looking', 'lookingfor', 'show',
        # Request words
        'want', 'need', 'find', 'search', 'give', 'get',
        'some', 'about', 'from', 'that', 'this', 'what', 'which', 'who',
        'can', 'could', 'would', 'should', 'will', 'may', 'might',
        'any', 'all', 'more', 'most', 'other', 'new', 'good', 'great',
        'read', 'something', 'anything', 'recommend', 'similar', 'like',
        'please', 'help', 'tell', 'list'
    }

    # Filler phrases that trail after a book title or author name
    _TRAILING_FILLER = re.compile(
        r'\s+(please|thanks|thank you|can you|could you|for me|i think|'
        r'i loved it|i enjoyed it|i really loved|i really enjoyed|'
        r'and similar|and others|or similar|or something like that|'
        r'type of books?|kind of books?|sort of books?).*$',
        re.IGNORECASE
    )

    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str, database: str = "neo4j"):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.database = database
        self.model_name = "qwen2.5:3b"
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

            if not any('qwen2.5:3b' in name.lower() for name in model_names):
                raise RuntimeError("Qwen not found. Run: ollama pull qwen2.5:3b")
        except Exception as e:
            raise Exception(f"Ollama check failed: {e}")

    def close(self):
        self.driver.close()

    # -----------------------------------------------------------------------
    # Data cleaning helpers
    # -----------------------------------------------------------------------

    def _clean_author(self, author: str) -> str:
        """
        Normalize author names from database format.
        "Patterson, James" -> "James Patterson"
        "Maas, Sarah J."   -> "Sarah J. Maas"
        """
        if not author:
            return ''
        author = author.strip()
        # Handle "Surname, Firstname" format
        if ',' in author:
            parts = [p.strip() for p in author.split(',', 1)]
            if len(parts) == 2 and parts[1]:
                return f"{parts[1]} {parts[0]}"
        return author

    def _clean_genre(self, genre: str) -> str:
        """Clean up genre/subject strings from the database."""
        if not genre:
            return ''
        # Remove anything in parentheses e.g. "Fiction (Fantasy)"
        genre = re.sub(r'\(.*?\)', '', genre).strip()
        # Title-case and strip trailing punctuation
        genre = genre.strip('.,;:').strip()
        return genre if genre else ''

    def _clean_books(self, books: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean author names and genre strings on a list of books."""
        cleaned = []
        for book in books:
            cleaned.append({
                **book,
                'authors': [self._clean_author(a) for a in book.get('authors', []) if a],
                'genres':  [self._clean_genre(g)  for g in book.get('genres', [])  if g],
            })
        return cleaned

    # -----------------------------------------------------------------------
    # Intent parsing helpers
    # -----------------------------------------------------------------------

    def _clean_term(self, term: str) -> str:
        """
        Clean an extracted author name or book title.
        Stop at the first comma, question mark, or exclamation mark.
        Remove trailing filler phrases like "please", "i loved it", etc.
        """
        term = re.split(r'[,?!]', term)[0]
        term = self._TRAILING_FILLER.sub('', term)
        term = re.sub(r'[^\w\s\'-]', '', term).strip()
        return term

    def _parse_query_intent(self, query: str) -> Tuple[str, str]:
        """Detect query intent: similar, author, or keyword."""
        query_lower = query.lower().strip()

        # --- Similar intent ---
        similar_patterns = [
            r'(?:books?\s+)?(?:like|similar\s+to|such\s+as)\s+(.+)',
            r'(?:recommend\s+)?(?:something|books?)\s+like\s+(.+)',
            r'if\s+(?:you\s+)?(?:like[d]?|enjoyed|loved)\s+(.+)',
            r'fans?\s+of\s+(.+)',
        ]
        for pattern in similar_patterns:
            match = re.search(pattern, query_lower)
            if match:
                term = self._clean_term(match.group(1).strip())
                if term:
                    return ("similar", term)

        # --- Author intent ---
        author_patterns = [
            r'(?:books?\s+)?(?:written\s+by|authored\s+by|by)\s+(.+)',
            r"(.+?)'s\s+books?",
            r'(?:works?|novels?|series)\s+(?:by|from)\s+(.+)',
            r'(?:author|writer)\s+(?:is\s+|named\s+)?(.+)',
        ]
        for pattern in author_patterns:
            match = re.search(pattern, query_lower)
            if match:
                term = self._clean_term(match.group(1).strip())
                book_words = ['harry', 'potter', 'hunger', 'games', 'narnia']
                if term and not any(word in term for word in book_words):
                    return ("author", term)

        return ("keyword", query)

    def _parse_query_logic(self, query: str) -> tuple:
        """
        Parse AND/OR logic from query.

        "magic AND dragons" -> (["magic", "dragons"], "AND")
        "witches OR wizards" -> (["witches", "wizards"], "OR")
        "fantasy adventure"  -> (["fantasy", "adventure"], "OR")
        """
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

    def _filter_keywords(self, query: str) -> List[str]:
        """Filter out stop words, punctuation, and normalize keywords."""
        # Strip all punctuation before splitting so "fantasy?" becomes "fantasy"
        query_clean = re.sub(r'[^\w\s]', ' ', query.lower())
        words = query_clean.split()
        keywords = [w for w in words if w not in self.STOP_WORDS and len(w) > 2]
        if not keywords:
            keywords = [w for w in words if len(w) > 2][:3]

        # Add singular/plural variants
        expanded = []
        for kw in (keywords if keywords else words[:3]):
            expanded.append(kw)
            if kw.endswith('s') and len(kw) > 3:
                expanded.append(kw[:-1])
            elif not kw.endswith('s'):
                expanded.append(kw + 's')

        return list(dict.fromkeys(expanded))

    # -----------------------------------------------------------------------
    # Retrieval methods
    # -----------------------------------------------------------------------

    def retrieve_books_by_keyword(self, query: str, limit: int = 10, debug: bool = False) -> List[Dict[str, Any]]:
        """Core method: Keyword search with AND/OR logic."""
        keywords, logic = self._parse_query_logic(query)

        if not keywords:
            if debug:
                print(f"  No keywords extracted from: {query}", flush=True)
            return []

        if debug:
            print(f"  Keywords: {keywords} (logic: {logic})", flush=True)

        with self.driver.session(database=self.database) as session:
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
                LIMIT 500
            """, keywords=keywords).data()

            if debug:
                print(f"  Stage 1 (CONTAINS): {len(result)} candidates", flush=True)

            books = []
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

                books.append({
                    'title': title,
                    'authors': authors,
                    'genres': genres,
                    'relevance': matches
                })

            books.sort(key=lambda x: x['relevance'], reverse=True)

            if debug:
                print(f"  Stage 2 (word boundary): {len(books)} matches", flush=True)

            return books[:limit]

    def retrieve_books_by_author(self, author_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Find books by author with name matching."""
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
                        'genres': [g for g in record['genres'] if g]
                    })
                    if len(books) >= limit:
                        break

            return books

    def retrieve_similar_books(self, book_title: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Find books similar to given title via graph traversal."""
        keywords = self._filter_keywords(book_title)

        if not keywords:
            return []

        with self.driver.session(database=self.database) as session:
            source_result = session.run("""
                MATCH (b:Book)
                WHERE ANY(keyword IN $keywords WHERE toLower(b.title) CONTAINS keyword)
                RETURN b.title AS title
                LIMIT 50
            """, keywords=keywords).data()

            source_title = None
            best_matches = 0
            for record in source_result:
                title_lower = (record['title'] or '').lower()
                matches = sum(1 for kw in keywords if re.search(r'\b' + re.escape(kw) + r'\b', title_lower))
                if matches > best_matches:
                    best_matches = matches
                    source_title = record['title']

            if not source_title:
                return []

            result = session.run("""
                MATCH (source:Book {title: $source_title})

                OPTIONAL MATCH (source)-[:HAS_SUBJECT]->(g:Genre)<-[:HAS_SUBJECT]-(similar:Book)
                WHERE source <> similar
                OPTIONAL MATCH (source)-[:WRITTEN_BY]->(a:Author)<-[:WRITTEN_BY]-(same_author:Book)
                WHERE source <> same_author

                WITH collect(DISTINCT similar) + collect(DISTINCT same_author) AS related
                UNWIND related AS book

                OPTIONAL MATCH (book)-[:WRITTEN_BY]->(ba:Author)
                OPTIONAL MATCH (book)-[:HAS_SUBJECT]->(bg:Genre)

                RETURN DISTINCT
                    book.title AS title,
                    collect(DISTINCT ba.author) AS authors,
                    collect(DISTINCT bg.subject_1) AS genres
                LIMIT $limit
            """, source_title=source_title, limit=limit).data()

            if not result:
                return []

            return [{
                'title': r['title'],
                'authors': [a for a in r['authors'] if a],
                'genres': [g for g in r['genres'] if g]
            } for r in result if r['title']]

    def smart_retrieve(self, query: str, limit: int = 5, debug: bool = True) -> Tuple[List[Dict[str, Any]], str]:
        """Smart retrieval based on query intent."""
        intent, term = self._parse_query_intent(query)

        if debug:
            print(f"  Intent: {intent}, Term: '{term}'", flush=True)

        if intent == "author":
            books = self.retrieve_books_by_author(term, limit)
            return books, "author"
        elif intent == "similar":
            books = self.retrieve_similar_books(term, limit)
            return books, "similar"
        else:
            books = self.retrieve_books_by_keyword(query, limit, debug=debug)
            return books, "keyword"

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

    # -----------------------------------------------------------------------
    # Response generation
    # -----------------------------------------------------------------------

    def generate_response(self, query: str, context: str, books: List[Dict[str, Any]] = None,
                          stream: bool = False, conversation_history: List[Dict[str, str]] = None,
                          intent: str = "keyword") -> str:
        """Generate response using Qwen with Chatalog persona."""

        is_first_message = not conversation_history or len(conversation_history) == 0

        if not books:
            no_results_msg = "I couldn't find any books matching your request in our catalog.\n\n"
            no_results_msg += "You can suggest a title for the library to add here:\n"
            no_results_msg += "https://www.spl.org/books-and-media/suggest-a-title\n\n"
            no_results_msg += "Or try a different search — maybe with different keywords?"
            if is_first_message:
                return "Hi! I'm Chatalog, a chatbot for the Seattle Public Library.\n\n" + no_results_msg
            return no_results_msg

        # Clean up data from the database before passing to Qwen
        books = self._clean_books(books)

        num_catalog_books = len(books)

        # Build the book list from cleaned database values only
        book_list = ""
        for i, book in enumerate(books, 1):
            book_list += f"\n{i}. {book.get('title', 'Unknown')}"
            if book.get('authors'):
                book_list += f" by {', '.join(book['authors'])}"
            if book.get('genres'):
                book_list += f" (Genres: {', '.join(book['genres'][:2])})"

        if intent == "author":
            task = (
                f"The patron is looking for books by a specific author. "
                f"List the {num_catalog_books} books found, grouped naturally. "
                f"Mention the author and genres briefly for each. Keep it conversational."
            )
        elif intent == "similar":
            task = (
                f"The patron wants books similar to a title they enjoyed. "
                f"List the {num_catalog_books} matching books and briefly explain "
                f"why each one is a good match based on genre or theme. Keep it friendly."
            )
        else:
            task = (
                f"List the {num_catalog_books} books found and briefly explain "
                f"why each fits the patron's request. Keep it concise and helpful."
            )

        greeting_instruction = (
            'Start your response with: "Hi! I\'m Chatalog, a chatbot for the Seattle Public Library."\n'
            if is_first_message else
            "Do not introduce yourself again — the patron already knows who you are.\n"
        )

        system_prompt = f"""You are Chatalog, a friendly readers' advisory chatbot for the Seattle Public Library.
{greeting_instruction}
{task}

Books found in the catalog:
{book_list}

Rules:
- Only reference the books listed above. Do not invent or suggest books outside this list.
- Use only the titles, authors, and genres as written above — do not alter or replace them.
- Be warm, concise, and helpful.
- Do not repeat the full list verbatim — describe each book naturally with a short synopsis and its genre.
- Do not use any markdown formatting. No asterisks, no hashes, no bullet symbols."""

        messages = [{"role": "system", "content": system_prompt}]
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": query})

        try:
            if stream:
                response_parts = []
                for chunk in ollama.chat(model=self.model_name, messages=messages, stream=True,
                                         options={"temperature": 0.7}):
                    if hasattr(chunk, 'message'):
                        text = chunk.message.content or ''
                    else:
                        text = chunk.get('message', {}).get('content', '')
                    print(text, end='', flush=True)
                    response_parts.append(text)
                print()
                response_text = ''.join(response_parts)
            else:
                response = ollama.chat(model=self.model_name, messages=messages,
                                       options={"temperature": 0.7})
                if hasattr(response, 'message'):
                    response_text = response.message.content
                else:
                    response_text = response['message']['content']

            # Strip markdown formatting Qwen may have added despite instructions
            response_text = response_text.replace('**', '').replace('*', '').replace('##', '').replace('# ', '')

            return response_text

        except Exception as e:
            return f"Error generating response: {e}"

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def recommend(self, query: str, retrieval_method: str = "smart", limit: int = 5,
                  stream: bool = False, conversation_history: List[Dict[str, str]] = None) -> Dict[str, Any]:
        print(f"\nQuery: {query}", flush=True)
        print(f"Method: {retrieval_method}\n", flush=True)

        if retrieval_method == "smart":
            books, method_used = self.smart_retrieve(query, limit)
        else:
            books = self.retrieve_books_by_keyword(query, limit)
            method_used = "keyword"

        print(f"Found {len(books)} books (method: {method_used})\n", flush=True)

        context = self.format_context(books)
        response = self.generate_response(
            query, context, books=books, stream=stream,
            conversation_history=conversation_history or [],
            intent=method_used
        )

        return {
            'query': query,
            'retrieval_method': method_used,
            'retrieved_books': books,
            'num_books_retrieved': len(books),
            'response': response
        }


def main():
    from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE

    system = GraphRAGQwen(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE)

    try:
        print("\n" + "="*60)
        print("Chatalog - Seattle Public Library Assistant")
        print("="*60)
        print("\nHi! I'm Chatalog, a chatbot for the Seattle Public Library.")
        print("I can help you find books in our catalog.\n")
        print("Try asking:")
        print("  - 'fantasy and magic books'")
        print("  - 'books by Diana Gabaldon'")
        print("  - 'books like Harry Potter'")
        print("  - 'does the library have books about dragons?'")
        print("\nType 'quit' to exit\n")

        while True:
            query = input("What are you looking for? ")

            if query.lower() in ['quit', 'exit', 'q']:
                print("\nThanks for visiting Chatalog! Happy reading!\n")
                break

            if not query.strip():
                continue

            result = system.recommend(query, retrieval_method="smart", limit=5, stream=True)
            print("\n")
    finally:
        system.close()


if __name__ == "__main__":
    main()