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
        'please', 'help', 'tell', 'list',
        'author', 'authors', 'writer', 'writers',
    }

    _TRAILING_FILLER = re.compile(
        r'\s+(please|thanks|thank you|can you|could you|for me|i think|'
        r'i loved it|i enjoyed it|i really loved|i really enjoyed|'
        r'and similar|and others|or similar|or something like that|'
        r'type of books?|kind of books?|sort of books?).*$',
        re.IGNORECASE
    )

    _MARC_NOISE = re.compile(
        r'\b(fictitious character|imaginary organization|comic books? strips? etc|'
        r'juvenile literature|juvenile delinquents|fictitious characters|'
        r'graphic novels|history and criticism|exhibitions|'
        r'social life and customs|personal narratives)\b',
        re.IGNORECASE
    )

    _GENRE_KEYWORDS = [
        'fantasy', 'science fiction', 'mystery', 'thriller', 'horror',
        'romance', 'historical fiction', 'adventure', 'juvenile fiction',
        'juvenile literature', 'young adult fiction', 'graphic novel',
        'nonfiction', 'biography', 'poetry', 'short stories',
        'fiction', 'humor', 'satire', 'drama',
    ]

    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str, database: str = "neo4j"):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.database = database
        self.model_name = "qwen2.5:3b"
        self._verify_connection()
        self._verify_ollama()
        self._build_field_index()

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

    # nationality adjectives → country values as stored in Neo4j
    _NATIONALITY_MAP = {
        'canadian':      'canada',
        'american':      'united states',
        'british':       'united kingdom',
        'english':       'united kingdom',
        'australian':    'australia',
        'irish':         'ireland',
        'scottish':      'scotland',
        'french':        'france',
        'german':        'germany',
        'japanese':      'japan',
        'russian':       'russia',
        'spanish':       'spain',
        'italian':       'italy',
        'chinese':       'china',
        'brazilian':     'brazil',
        'mexican':       'mexico',
        'swedish':       'sweden',
        'norwegian':     'norway',
        'danish':        'denmark',
        'dutch':         'netherlands',
        'portuguese':    'portugal',
        'polish':        'poland',
        'indian':        'india',
        'nigerian':      'nigeria',
        'ghanaian':      'ghana',
        'kenyan':        'kenya',
    }

    def _build_field_index(self):
        """
        Build a lookup dict from query keywords to (node, property, value)
        using whatever values actually exist in the graph.
        """
        self._field_index = {}

        with self.driver.session(database=self.database) as session:
            # Book languages
            langs = session.run("""
                MATCH (b:Book)
                WHERE b.language IS NOT NULL AND b.language <> ""
                  AND size(b.language) < 50
                RETURN DISTINCT toLower(b.language) AS val
            """).data()
            for row in langs:
                val = row['val'].strip()
                if val:
                    self._field_index[val] = ('Book', 'language', val)

            # Author countries — stored values may be comma-separated
            countries = session.run("""
                MATCH (a:Author)
                WHERE a.country IS NOT NULL AND a.country <> ""
                RETURN DISTINCT a.country AS val
            """).data()
            for row in countries:
                for country in row['val'].split(','):
                    country = country.strip().lower()
                    if country:
                        self._field_index[country] = ('Author', 'country', country)

            # Author occupations — comma-separated
            occs = session.run("""
                MATCH (a:Author)
                WHERE a.occupation IS NOT NULL AND a.occupation <> ""
                RETURN DISTINCT a.occupation AS val
            """).data()
            for row in occs:
                for occ in row['val'].split(','):
                    occ = occ.strip().lower()
                    if len(occ) > 3:
                        self._field_index[occ] = ('Author', 'occupation', occ)

        # Add nationality adjectives that map to country values
        for adjective, country_value in self._NATIONALITY_MAP.items():
            if country_value in self._field_index:
                self._field_index[adjective] = ('Author', 'country', country_value)

        print(f"Field index built: {len(self._field_index)} entries", flush=True)

    # -----------------------------------------------------------------------
    # Data cleaning helpers
    # -----------------------------------------------------------------------

    def _clean_author(self, author: str) -> str:
        """
        Normalize author names from database format.
        "Patterson, James, 1947-" -> "James Patterson"
        "Maas, Sarah J."          -> "Sarah J. Maas"
        """
        if not author:
            return ''
        author = author.strip()
        author = re.sub(r',?\s*\d{4}(-\d{4})?', '', author)
        if ',' in author:
            parts = [p.strip() for p in author.split(',', 1)]
            if len(parts) == 2 and parts[1]:
                return f"{parts[1]} {parts[0]}"
        return author

    def _clean_genre(self, genre: str) -> str:
        """Extract a clean genre label from messy MARC subject heading strings."""
        if not genre:
            return ''
        genre = re.sub(r'\(.*?\)', '', genre).strip()
        genre_lower = genre.lower()

        for kw in sorted(self._GENRE_KEYWORDS, key=len, reverse=True):
            if kw in genre_lower:
                return kw.title()

        words = genre.split()
        if len(words) <= 3 and not self._MARC_NOISE.search(genre):
            return genre.strip('.,;:').strip()

        return ''

    def _clean_books(self, books: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean author names and genre strings on a list of books."""
        cleaned = []
        for book in books:
            clean_genres = [self._clean_genre(g) for g in book.get('genres', []) if g]
            seen = set()
            deduped_genres = []
            for g in clean_genres:
                if g and g not in seen:
                    seen.add(g)
                    deduped_genres.append(g)
            cleaned.append({
                **book,
                'authors': list(dict.fromkeys(
                    self._clean_author(a) for a in book.get('authors', []) if a
                )),
                'genres': deduped_genres,
            })
        return cleaned

    # ---------------------------
    # Keyword handling
    # ---------------------------

    def _expand_keyword(self, kw: str) -> List[str]:
        variants = [kw]
        if kw.endswith('y'):
            variants.append(kw[:-1] + 'ies')
        elif not kw.endswith('s'):
            variants.append(kw + 's')
        return variants
        
    def _filter_keywords(self, query: str) -> List[str]:
        words = re.sub(r'[^\w\s]', ' ', query.lower()).split()
        keywords = [w for w in words if w not in self.STOP_WORDS and len(w) > 2]

        expanded = []
        for kw in keywords:
            expanded.extend(self._expand_keyword(kw))

        return list(dict.fromkeys(expanded))
    
    # -----------------------------------------------------------------------
    # Intent parsing helpers
    # -----------------------------------------------------------------------

    def _clean_term(self, term: str) -> str:
        term = re.split(r'[,?!]', term)[0]
        term = self._TRAILING_FILLER.sub('', term)
        term = re.sub(r'[^\w\s\'-]', '', term).strip()
        return term

    def _parse_query_intent(self, query: str) -> Tuple[str, Any]:
        query_lower = query.lower().strip()

        # --- check field index first (language, country, occupation) ---
        # Use base words without plural expansion so "canadians" doesn't block "canadian"
        base_words = [w for w in re.sub(r'[^\w\s]', ' ', query.lower()).split()
                      if w not in self.STOP_WORDS and len(w) > 2]
        field_kws = [w for w in base_words if w in self._field_index]
        content_kws = [w for w in base_words if w not in self._field_index]
        print(f"  Field index size: {len(self._field_index)}, base_words: {base_words}, field_kws: {field_kws}, content_kws: {content_kws}", flush=True)
        if field_kws and not content_kws:
            node, prop, val = self._field_index[field_kws[0]]
            return ("field_filter", {'node': node, 'prop': prop, 'value': val})

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
                if len(term.split()) <= 4:
                    return "author", term

        return ("keyword", query)

    def _parse_query_logic(self, query: str) -> tuple:
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

    # -----------------------------------------------------------------------
    # Retrieval methods
    # -----------------------------------------------------------------------

    def retrieve_books_by_keyword(self, query: str, limit: int = 10, debug: bool = False) -> List[Dict[str, Any]]:
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
                WITH 
                    b,
                    collect(DISTINCT a.author) AS authors_rel,
                    collect(DISTINCT g.subject_1) AS genres_rel,
                    // --- ENRICHMENT ---
                    coalesce(b.author, "") AS author_wiki,
                    coalesce(b.genre, "") AS genre_wiki,
                    coalesce(b.subject, "") AS subject_wiki,
                    coalesce(b.language, "") AS language_wiki,
                    toLower(b.title) AS title_lower
                WITH 
                    b,
                    authors_rel + CASE WHEN author_wiki <> "" THEN [author_wiki] ELSE [] END AS authors,
                    genres_rel + 
                        CASE WHEN genre_wiki <> "" THEN [genre_wiki] ELSE [] END +
                        CASE WHEN subject_wiki <> "" THEN [subject_wiki] ELSE [] END AS genres,
                    title_lower,
                    toLower(language_wiki) AS language_lower
                WHERE ANY(keyword IN $keywords WHERE
                    title_lower CONTAINS keyword OR
                    ANY(g IN genres WHERE toLower(g) CONTAINS keyword) OR
                    ANY(a IN authors WHERE toLower(a) CONTAINS keyword) OR
                    language_lower CONTAINS keyword
                )
                WITH b, authors, genres, language_lower
                RETURN 
                    b.title AS title,
                    authors,
                    genres,
                    language_lower
                LIMIT 500
            """, keywords=keywords).data()

            books = []
    
            for record in result:
                title = record.get('title') or ''
                authors = [a for a in (record.get('authors') or []) if a]
                genres = [g for g in (record.get('genres') or []) if g]
                language = record.get('language_lower') or ''
    
                matches = 0
                score = 0
    
                for kw in keywords:
                    if re.search(r'\b' + re.escape(kw) + r'\b', title.lower()):
                        matches += 1
                        score += 3
                    elif any(kw in g.lower() for g in genres):
                        matches += 1
                        score += 2
                    elif kw in language:
                        matches += 1
                        score += 2
                    elif any(kw in a.lower() for a in authors):
                        matches += 1
                        score += 1

    
                if logic == "AND" and matches < len(keywords):
                    continue
                if logic == "OR" and matches == 0:
                    continue
    
                books.append({
                    'title': title,
                    'authors': authors,
                    'genres': genres,
                    'relevance': score  # --- FIX: better ranking ---
                })
    
            books.sort(key=lambda x: x['relevance'], reverse=True)
            return books[:limit]

    def retrieve_books_by_author(self, author_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        cleaned = author_name.lower().replace('.', ' ').replace(',', ' ')
        words = [p.strip() for p in cleaned.split() if p.strip()]
    
        surname = max(words, key=len) if words else author_name.lower()
    
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WITH 
                    b,
                    collect(DISTINCT a.author) AS authors_rel,
                    collect(DISTINCT g.subject_1) AS genres_rel,
                    // --- ENRICHMENT ---
                    coalesce(b.author, "") AS author_wiki,
                    coalesce(b.genre, "") AS genre_wiki,
                    coalesce(b.subject, "") AS subject_wiki
                WITH 
                    b,
                    authors_rel + CASE WHEN author_wiki <> "" THEN [author_wiki] ELSE [] END AS authors,
                    genres_rel + 
                        CASE WHEN genre_wiki <> "" THEN [genre_wiki] ELSE [] END +
                        CASE WHEN subject_wiki <> "" THEN [subject_wiki] ELSE [] END AS genres
                WHERE ANY(a IN authors WHERE toLower(a) CONTAINS $surname)
                RETURN 
                    b.title AS title,
                    authors,
                    genres
                LIMIT 50
            """, surname=surname).data()
    
            return [{
                'title': r.get('title'),
                'authors': [a for a in (r.get('authors') or []) if a],
                'genres': [g for g in (r.get('genres') or []) if g]
            } for r in result if r.get('title')][:limit]

    def retrieve_similar_books(self, book_title: str, limit: int = 5) -> List[Dict[str, Any]]:
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
                    collect(DISTINCT ba.author) +
                        CASE WHEN book.author IS NOT NULL THEN [book.author] ELSE [] END
                        AS authors,
                    collect(DISTINCT bg.subject_1) +
                        CASE WHEN book.genre IS NOT NULL THEN [book.genre] ELSE [] END +
                        CASE WHEN book.subject IS NOT NULL THEN [book.subject] ELSE [] END
                        AS genres
                LIMIT $limit
            """, source_title=source_title, limit=limit).data()

            if not result:
                return []

            return [{
                'title': r['title'],
                'authors': [a for a in r['authors'] if a],
                'genres': [g for g in r['genres'] if g]
            } for r in result if r['title']]

    def retrieve_books_by_field(self, node: str, prop: str, value: str, limit: int = 10) -> List[Dict[str, Any]]:
        if node == 'Book':
            query = """
                MATCH (b:Book)
                WHERE toLower(coalesce(b[$prop], '')) CONTAINS $value
                  AND size(coalesce(b[$prop], '')) < 50
                OPTIONAL MATCH (b)-[:WRITTEN_BY]->(a:Author)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                RETURN b.title AS title,
                       collect(DISTINCT a.author) AS authors,
                       collect(DISTINCT g.subject_1) AS genres
                LIMIT $limit
            """
        else:
            query = """
                MATCH (b:Book)-[:WRITTEN_BY]->(a:Author)
                WHERE toLower(coalesce(a[$prop], '')) CONTAINS $value
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                RETURN b.title AS title,
                       collect(DISTINCT a.author) AS authors,
                       collect(DISTINCT g.subject_1) AS genres
                LIMIT $limit
            """
        with self.driver.session(database=self.database) as session:
            result = session.run(query, prop=prop, value=value, limit=limit).data()
        return [{
            'title': r['title'],
            'authors': [a for a in r['authors'] if a],
            'genres': [g for g in r['genres'] if g],
        } for r in result if r['title']]

    def smart_retrieve(self, query: str, limit: int = 5, debug: bool = True) -> Tuple[List[Dict[str, Any]], str]:
        intent, term = self._parse_query_intent(query)

        if debug:
            print(f"  Intent: {intent}, Term: '{term}'", flush=True)

        if intent == "author":
            books = self.retrieve_books_by_author(term, limit)
            return books, "author"
        elif intent == "similar":
            books = self.retrieve_similar_books(term, limit)
            return books, "similar"
        elif intent == "field_filter":
            books = self.retrieve_books_by_field(
                term['node'], term['prop'], term['value'], limit
            )
            return books, "keyword"
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
    # Description generation — Qwen's ONLY job
    # -----------------------------------------------------------------------

    def _get_descriptions(self, query: str, books: List[Dict[str, Any]],
                          conversation_history: List[Dict[str, str]],
                          intent: str) -> List[str]:
        """
        Ask Qwen for a short 2-3 sentence blurb per book.
        Genre info must be woven naturally into the description.
        Qwen never outputs titles — it only writes the blurbs.
        """
        numbered = "\n".join(
            f"{i}. \"{b['title']}\" "
            f"(genres: {', '.join(b['genres'][:3]) if b.get('genres') else 'general fiction'})"
            for i, b in enumerate(books, 1)
        )

        if intent == "author":
            task = "The patron is looking for books by a specific author."
        elif intent == "similar":
            task = "The patron wants books similar to one they enjoyed."
        else:
            task = "The patron is searching for books matching their interests."

        prompt = (
            f"A library patron asked: \"{query}\"\n"
            f"{task}\n\n"
            f"For each book below, write 2 to 3 sentences that:\n"
            f"- Describe what the book is about in an engaging way\n"
            f"- Naturally mention the genre or feel of the book without using the word 'genre'\n"
            f"- Explain briefly why it suits the patron's request\n\n"
            f"Return ONLY a numbered list — one blurb per book, nothing else.\n"
            f"Do not repeat the book title in your response. "
            f"Do not add extra books. Do not use asterisks or markdown.\n\n"
            f"Books:\n{numbered}"
        )

        try:
            response = ollama.generate(
                model=self.model_name,
                prompt=prompt,
                options={"temperature": 0.6}
            )
            if isinstance(response, dict):
                raw = response.get('response', '')
            else:
                raw = getattr(response, 'response', '') or ''

            raw = raw.replace('**', '').replace('*', '').replace('##', '')

            # Parse numbered entries — each may span multiple lines
            descriptions = []
            current = []
            for line in raw.strip().splitlines():
                line = line.strip()
                if re.match(r'^\d+[\.\)]\s+', line):
                    if current:
                        descriptions.append(' '.join(current).strip())
                    # Strip the leading number
                    current = [re.sub(r'^\d+[\.\)]\s+', '', line)]
                elif line and current:
                    current.append(line)
            if current:
                descriptions.append(' '.join(current).strip())

            while len(descriptions) < len(books):
                descriptions.append('')

            return descriptions[:len(books)]

        except Exception as e:
            print(f"  Description generation failed: {e}", flush=True)
            return ['' for _ in books]

    # -----------------------------------------------------------------------
    # Intro line generation — warm filler before the count
    # -----------------------------------------------------------------------

    def _get_intro(self, query: str, intent: str, num_books: int,
                   conversation_history: List[Dict[str, str]]) -> str:
        """
        Ask Qwen for a short 1-2 sentence warm intro that fits the query,
        to appear before 'I found X books in our catalog'.
        """
        if intent == "author":
            context = "The patron is looking for books by a specific author."
        elif intent == "similar":
            context = "The patron wants books similar to one they have enjoyed."
        else:
            context = "The patron is searching for books on a topic they are interested in."

        prompt = (
            f"A library patron asked: \"{query}\"\n"
            f"{context}\n\n"
            f"Write 1 to 2 warm, friendly sentences acknowledging their request before "
            f"a list of results is shown. Do not mention how many books were found. "
            f"Do not list any book titles. Do not use asterisks or markdown. "
            f"Keep it brief and natural, like a helpful librarian would say."
        )

        try:
            response = ollama.generate(
                model=self.model_name,
                prompt=prompt,
                options={"temperature": 0.7}
            )
            if isinstance(response, dict):
                raw = response.get('response', '')
            else:
                raw = getattr(response, 'response', '') or ''

            raw = raw.replace('**', '').replace('*', '').replace('##', '').strip()
            # Only keep first 2 sentences max
            sentences = re.split(r'(?<=[.!?])\s+', raw)
            return ' '.join(sentences[:2]).strip()

        except Exception:
            return "Great question — let me check what we have in our catalog for you."

    # -----------------------------------------------------------------------
    # Response generation — Python assembles the final string
    # -----------------------------------------------------------------------

    def generate_response(self, query: str, context: str, books: List[Dict[str, Any]] = None,
                          stream: bool = False, conversation_history: List[Dict[str, str]] = None,
                          intent: str = "keyword") -> str:
        """
        Python assembles the entire formatted response.
        Qwen writes the intro filler and per-book blurbs only.
        Titles and authors always come from Neo4j.
        """

        is_first_message = not conversation_history or len(conversation_history) == 0

        if not books:
            no_results_msg = "I couldn't find any books matching your request in our catalog.\n\n"
            no_results_msg += "You can suggest a title for the library to add here:\n"
            no_results_msg += "https://www.spl.org/books-and-media/suggest-a-title\n\n"
            no_results_msg += "Or try a different search — maybe with different keywords?"
            if is_first_message:
                return "Hi! I'm Chatalog, a chatbot for the Seattle Public Library.\n\n" + no_results_msg
            return no_results_msg

        # Clean data from the database
        books = self._clean_books(books)
        num_catalog_books = len(books)

        # Get per-book blurbs and the intro line from Qwen
        descriptions = self._get_descriptions(query, books, conversation_history or [], intent)
        intro = self._get_intro(query, intent, num_catalog_books, conversation_history or [])

        # --- Assemble the response entirely in Python ---
        lines = []

        if is_first_message:
            lines.append("Hi! I'm Chatalog, a chatbot for the Seattle Public Library.")
            lines.append("")

        lines.append(intro)
        lines.append("")
        lines.append(
            f"I found {num_catalog_books} book{'s' if num_catalog_books != 1 else ''} "
            f"in our catalog that match your request:"
        )
        lines.append("")

        for i, (book, desc) in enumerate(zip(books, descriptions), 1):
            title   = book.get('title', 'Unknown')
            authors = ', '.join(book['authors']) if book.get('authors') else 'Unknown author'

            lines.append(f"{i}. {title} by {authors}")

            if desc:
                lines.append(f"   {desc}")

            lines.append("")

        lines.append(
            "If you don't see what you're looking for, you can suggest a title "
            "for the library to add here:"
        )
        lines.append("https://www.spl.org/books-and-media/suggest-a-title")

        return "\n".join(lines)

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