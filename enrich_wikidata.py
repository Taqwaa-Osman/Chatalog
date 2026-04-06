from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON, POST
import os
import time
import re

# ---------------------------
# CONFIG
# ---------------------------
NEO4J_URI = os.getenv("NEO4J_URI", "neo4j+s://2ec65fcd.databases.neo4j.io")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "aFhEUnY8Wp05kqpx3-NLq9UHnPJO5mFY04pPNy1-1ag")

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

BATCH_SIZE = 20
SLEEP = 2

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ---------------------------
# UTIL
# ---------------------------
def create_sparql():
    sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
    sparql.setReturnFormat(JSON)
    sparql.setTimeout(30)
    sparql.setMethod(POST)
    sparql.addCustomHttpHeader(
        "User-Agent",
        "ChatalogKGEnrichment (hodankandid@cmail.carleton.ca)"
    )
    return sparql


def escape_string(s):
    if not s:
        return None
    s = s.replace("\\", "\\\\")
    s = s.replace('"', '\\"')
    return s


def clean_name(name):
    if not name:
        return None

    name = re.sub(r"\(.*?\)", "", name)
    name = re.sub(r",?\s*\d{4}(-\d{4})?", "", name)

    if "," in name:
        parts = [p.strip() for p in name.split(",")]
        if len(parts) >= 2:
            name = parts[1] + " " + parts[0]

    return name.strip()


def is_valid_name(name):
    if not name or len(name) > 80:
        return False

    bad_keywords = [
        "committee", "department", "agency",
        "production", "films", "bureau",
        "congress", "subcommittee"
    ]

    return not any(k in name.lower() for k in bad_keywords)


def safe_get(b, field):
    return b.get(field, {}).get("value")


# ---------------------------
# AUTHOR QUERY (MWAPI)
# ---------------------------
def query_authors_batch(batch):
    sparql = create_sparql()

    values = []
    for original in batch:
        if not is_valid_name(original):
            continue

        cleaned = clean_name(original)
        if cleaned:
            o = escape_string(original)
            c = escape_string(cleaned)
            values.append(f'("{o}" "{c}")')

    if not values:
        return None

    values_block = "\n".join(values)

    query = f"""
    SELECT ?inputName ?author ?authorLabel
           (GROUP_CONCAT(DISTINCT ?countryLabel; separator=", ") AS ?country)
           (GROUP_CONCAT(DISTINCT ?occupationLabel; separator=", ") AS ?occupation)
           (SAMPLE(?birth) AS ?birth)
           (SAMPLE(?death) AS ?death)
    WHERE {{
      VALUES (?inputName ?searchTerm) {{
        {values_block}
      }}

      SERVICE wikibase:mwapi {{
        bd:serviceParam wikibase:endpoint "www.wikidata.org" ;
                        wikibase:api "EntitySearch" ;
                        mwapi:search ?searchTerm ;
                        mwapi:language "en" .
        ?author wikibase:apiOutputItem mwapi:item .
      }}

      ?author wdt:P31 wd:Q5 .

      OPTIONAL {{ ?author wdt:P569 ?birth }}
      OPTIONAL {{ ?author wdt:P570 ?death }}

      OPTIONAL {{
        ?author wdt:P27 ?country .
        ?country rdfs:label ?countryLabel FILTER (lang(?countryLabel)="en")
      }}

      OPTIONAL {{
        ?author wdt:P106 ?occupation .
        ?occupation rdfs:label ?occupationLabel FILTER (lang(?occupationLabel)="en")
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    GROUP BY ?inputName ?author ?authorLabel
    """

    try:
        sparql.setQuery(query)
        return sparql.query().convert()
    except Exception as e:
        print("Author batch failed:", e)
        return None


# ---------------------------
# QID-BASED AUTHOR ENRICHMENT
# ---------------------------
def query_authors_by_qid(qid_batch):
    sparql = create_sparql()

    values_block = " ".join([f"wd:{qid}" for qid in qid_batch])

    query = f"""
    SELECT ?author ?authorLabel
           (GROUP_CONCAT(DISTINCT ?countryLabel; separator=", ") AS ?country)
           (GROUP_CONCAT(DISTINCT ?occupationLabel; separator=", ") AS ?occupation)
           (SAMPLE(?birth) AS ?birth)
           (SAMPLE(?death) AS ?death)
    WHERE {{
      VALUES ?author {{ {values_block} }}

      OPTIONAL {{ ?author wdt:P569 ?birth }}
      OPTIONAL {{ ?author wdt:P570 ?death }}

      OPTIONAL {{
        ?author wdt:P27 ?country .
        ?country rdfs:label ?countryLabel FILTER (lang(?countryLabel)="en")
      }}

      OPTIONAL {{
        ?author wdt:P106 ?occupation .
        ?occupation rdfs:label ?occupationLabel FILTER (lang(?occupationLabel)="en")
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    GROUP BY ?author ?authorLabel
    """

    try:
        sparql.setQuery(query)
        return sparql.query().convert()
    except Exception as e:
        print("QID batch failed:", e)
        return None


# ---------------------------
# DB HELPERS
# ---------------------------
def get_authors(tx):
    return [r["name"] for r in tx.run("""
        MATCH (a:Author)
        WHERE a.wikidata_id IS NULL
        RETURN a.author AS name
    """)]


# authors needing second pass
def get_authors_missing_details(tx):
    return tx.run("""
        MATCH (a:Author)
        WHERE a.wikidata_id IS NOT NULL
          AND (a.country IS NULL OR a.occupation IS NULL)
        RETURN a.author AS name, a.wikidata_id AS qid
    """).data()


# ---------------------------
# UPDATE FUNCTIONS
# ---------------------------
def update_author(tx, name, data):
    tx.run("""
        MATCH (a:Author {author: $name})
        SET a.wikidata_id = $qid,
            a.birth_date = COALESCE($birth, a.birth_date),
            a.death_date = COALESCE($death, a.death_date),
            a.country = CASE WHEN $country <> "" THEN $country ELSE a.country END,
            a.occupation = CASE WHEN $occupation <> "" THEN $occupation ELSE a.occupation END,
            a.source = "Wikidata"
    """, name=name, **data)


def update_author_qid(tx, qid, data):
    tx.run("""
        MATCH (a:Author {wikidata_id: $qid})
        SET a.birth_date = COALESCE($birth, a.birth_date),
            a.death_date = COALESCE($death, a.death_date),
            a.country = CASE WHEN $country <> "" THEN $country ELSE a.country END,
            a.occupation = CASE WHEN $occupation <> "" THEN $occupation ELSE a.occupation END
    """, qid=qid, **data)

# ---------------------------
# BOOK QID ENRICHMENT
# ---------------------------
def query_books_by_qid(qid_batch):
    sparql = create_sparql()

    values_block = " ".join([f"wd:{qid}" for qid in qid_batch])

    query = f"""
    SELECT ?book
           (GROUP_CONCAT(DISTINCT ?authorLabel; separator=", ") AS ?author)
           (GROUP_CONCAT(DISTINCT ?genreLabel; separator=", ") AS ?genre)
           (GROUP_CONCAT(DISTINCT ?subjectLabel; separator=", ") AS ?subject)
           (GROUP_CONCAT(DISTINCT ?languageLabel; separator=", ") AS ?language)
           (GROUP_CONCAT(DISTINCT ?seriesLabel; separator=", ") AS ?series)
           (GROUP_CONCAT(DISTINCT ?characterLabel; separator=", ") AS ?characters)
           (GROUP_CONCAT(DISTINCT ?locationLabel; separator=", ") AS ?location)

           (SAMPLE(?pubDate) AS ?pubDate)
           (SAMPLE(?publisherLabel) AS ?publisher)
           (SAMPLE(?pages) AS ?pages)

           (SAMPLE(?isbn) AS ?isbn)
           (SAMPLE(?oclc) AS ?oclc)
           (SAMPLE(?openlib) AS ?openlib)
           (SAMPLE(?google) AS ?google)

           (SAMPLE(?dewey) AS ?dewey)
           (SAMPLE(?loc) AS ?loc)

    WHERE {{
      VALUES ?book {{ {values_block} }}

      OPTIONAL {{
        ?book wdt:P50 ?a .
        ?a rdfs:label ?authorLabel FILTER (lang(?authorLabel)="en")
      }}

      OPTIONAL {{
        ?book wdt:P136 ?g .
        ?g rdfs:label ?genreLabel FILTER (lang(?genreLabel)="en")
      }}

      OPTIONAL {{
        ?book wdt:P921 ?s .
        ?s rdfs:label ?subjectLabel FILTER (lang(?subjectLabel)="en")
      }}

      OPTIONAL {{
        ?book wdt:P407 ?lang .
        ?lang rdfs:label ?languageLabel FILTER (lang(?languageLabel)="en")
      }}

      OPTIONAL {{
        ?book wdt:P179 ?ser .
        ?ser rdfs:label ?seriesLabel FILTER (lang(?seriesLabel)="en")
      }}

      OPTIONAL {{
        ?book wdt:P674 ?char .
        ?char rdfs:label ?characterLabel FILTER (lang(?characterLabel)="en")
      }}

      OPTIONAL {{
        ?book wdt:P840 ?loca .
        ?loca rdfs:label ?locationLabel FILTER (lang(?locationLabel)="en")
      }}

      OPTIONAL {{ ?book wdt:P577 ?pubDate }}

      OPTIONAL {{
        ?book wdt:P123 ?p .
        ?p rdfs:label ?publisherLabel FILTER (lang(?publisherLabel)="en")
      }}

      OPTIONAL {{ ?book wdt:P1104 ?pages }}

      OPTIONAL {{ ?book wdt:P212 ?isbn }}
      OPTIONAL {{ ?book wdt:P243 ?oclc }}
      OPTIONAL {{ ?book wdt:P648 ?openlib }}
      OPTIONAL {{ ?book wdt:P675 ?google }}

      OPTIONAL {{ ?book wdt:P1036 ?dewey }}
      OPTIONAL {{ ?book wdt:P1149 ?loc }}
    }}
    GROUP BY ?book
    """

    try:
        sparql.setQuery(query)
        return sparql.query().convert()
    except Exception as e:
        print("Book QID batch failed:", e)
        return None


# ---------------------------
# DB HELPERS
# ---------------------------
def get_books_missing_details(tx):
    return tx.run("""
        MATCH (b:Book)
        WHERE b.wikidata_id IS NOT NULL
        RETURN b.title AS title, b.wikidata_id AS qid
    """).data()


# ---------------------------
# UPDATE FUNCTION
# ---------------------------
def update_book_qid(tx, qid, data):
    tx.run("""
        MATCH (b:Book {wikidata_id: $qid})
        SET 
            b.author = COALESCE($author, b.author),
            b.genre = COALESCE($genre, b.genre),
            b.subject = COALESCE($subject, b.subject),
            b.language = COALESCE($language, b.language),
            b.series = COALESCE($series, b.series),
            b.characters = COALESCE($characters, b.characters),
            b.narrative_location = COALESCE($location, b.narrative_location),

            b.publication_date = COALESCE($publication_date, b.publication_date),
            b.publisher = COALESCE($publisher, b.publisher),
            b.pages = COALESCE($pages, b.pages),

            b.isbn = COALESCE($isbn, b.isbn),
            b.oclc = COALESCE($oclc, b.oclc),
            b.openlibrary_id = COALESCE($openlib, b.openlibrary_id),
            b.google_books_id = COALESCE($google, b.google_books_id),

            b.dewey = COALESCE($dewey, b.dewey),
            b.loc_classification = COALESCE($loc, b.loc_classification),

            b.source = "Wikidata"
    """, qid=qid, **data)

# ---------------------------
# MAIN
# ---------------------------
with driver.session() as session:

    # ---------------------------
    # PASS 1: NAME MATCHING
    # ---------------------------
    authors = session.execute_read(get_authors)
    print(f"Authors: {len(authors)}")

    for i in range(0, len(authors), BATCH_SIZE):
        batch = authors[i:i+BATCH_SIZE]
        results = query_authors_batch(batch)

        if results:
            for b in results["results"]["bindings"]:
                original = safe_get(b, "inputName")

                data = {
                    "qid": safe_get(b, "author").split("/")[-1],
                    "birth": safe_get(b, "birth"),
                    "death": safe_get(b, "death"),
                    "country": safe_get(b, "country"),
                    "occupation": safe_get(b, "occupation"),
                }

                session.execute_write(update_author, original, data)

        print(f"Authors batch {i}")
        time.sleep(SLEEP)

    # ---------------------------
    # PASS 2: QID ENRICHMENT
    # ---------------------------
    missing = session.execute_read(get_authors_missing_details)
    print(f"\nSecond pass (QID enrichment): {len(missing)} authors")

    qid_map = {row["qid"]: row["name"] for row in missing}
    qids = list(qid_map.keys())

    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i:i+BATCH_SIZE]
        results = query_authors_by_qid(batch)

        if results:
            for b in results["results"]["bindings"]:
                qid = safe_get(b, "author").split("/")[-1]

                data = {
                    "birth": safe_get(b, "birth"),
                    "death": safe_get(b, "death"),
                    "country": safe_get(b, "country"),
                    "occupation": safe_get(b, "occupation"),
                }

                session.execute_write(update_author_qid, qid, data)

        print(f"QID batch {i}")
        time.sleep(SLEEP)
        
        books = session.execute_read(get_books_missing_details)
        print(f"Books with QIDs: {len(books)}")

        qids = [row["qid"] for row in books]

        for i in range(0, len(qids), BATCH_SIZE):
            batch = qids[i:i+BATCH_SIZE]
            results = query_books_by_qid(batch)

            if results:
                for b in results["results"]["bindings"]:
                    qid = safe_get(b, "book").split("/")[-1]

                    data = {
                        "author": safe_get(b, "author"),
                        "genre": safe_get(b, "genre"),
                        "subject": safe_get(b, "subject"),
                        "language": safe_get(b, "language"),
                        "series": safe_get(b, "series"),
                        "characters": safe_get(b, "characters"),
                        "location": safe_get(b, "location"),

                        "publication_date": safe_get(b, "pubDate"),
                        "publisher": safe_get(b, "publisher"),
                        "pages": safe_get(b, "pages"),

                        "isbn": safe_get(b, "isbn"),
                        "oclc": safe_get(b, "oclc"),
                        "openlib": safe_get(b, "openlib"),
                        "google": safe_get(b, "google"),

                        "dewey": safe_get(b, "dewey"),
                        "loc": safe_get(b, "loc"),
                    }

                    session.execute_write(update_book_qid, qid, data)

            print(f"Processed batch {i}")
            time.sleep(SLEEP)
driver.close()
