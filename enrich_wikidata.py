from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import time
import re

NEO4J_URI = os.getenv("NEO4J_URI", "neo4j+s://2ec65fcd.databases.neo4j.io")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "aFhEUnY8Wp05kqpx3-NLq9UHnPJO5mFY04pPNy1-1ag")

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

# clean author names
def clean_author_name(name):
    if not name:
        return None

    name = re.sub(r"\(.*?\)", "", name) # take out parantheses

    name = re.sub(r",?\s*\d{4}(-\d{4})?", "", name)  # remove birth/death years
    name = re.sub(r"-\s*$", "", name)  # remove trailing dash in case its still left (messes up data scrapping if i don't do this)

    name = name.strip()

    # change "last, first" into "first last" (wrong order!!)
    if "," in name:
        parts = [p.strip() for p in name.split(",")]
        if len(parts) >= 2:
            name = parts[1] + " " + parts[0]

    return name.strip()

# get authors missing Wikidata
def get_authors(tx):
    query = """
    MATCH (a:Author)
    WHERE a.wikidata_id IS NULL
    RETURN a.author AS name
    LIMIT 20
    """
    return [record["name"] for record in tx.run(query)]

# query Wikidata
def query_wikidata(author_name):

    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

    # REQUIRED: User-Agent per Wikidata policy
    sparql.addCustomHttpHeader(
        "User-Agent",
        "ChatalogKGEnrichment/1.0 (hodan.kandid@cmail.carleton.ca)"
    )

    sparql.setTimeout(30)

    query = f"""
    SELECT ?author ?authorLabel ?birth WHERE {{
        ?author wdt:P31 wd:Q5 .
        ?author rdfs:label "{author_name}"@en .
        OPTIONAL {{ ?author wdt:P569 ?birth }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    return sparql.query().convert()

# go through results
def parse_result(results):
    bindings = results["results"]["bindings"]

    if not bindings:
        return None, None

    author_uri = bindings[0]["author"]["value"]
    qid = author_uri.split("/")[-1]

    birth = bindings[0].get("birth", {}).get("value")

    return qid, birth

# update Neo4j
def update_author(tx, original_name, qid, birth):
    query = """
    MATCH (a:Author {author: $original_name})
    SET a.wikidata_id = $qid,
        a.birth_date = $birth,
        a.source = "Wikidata"
    """
    tx.run(query, original_name=original_name, qid=qid, birth=birth)

# MAIN
with driver.session() as session:

    authors = session.execute_read(get_authors)

    print(f"Found {len(authors)} authors to enrich")

    for author in authors:

        cleaned_name = clean_author_name(author)

        if not cleaned_name:
            continue

        skip_keywords = [
            "production",
            "features",
            "university",
            "commission",
            "bureau",
            "animation",
            "editors",
            "group",
        ]

        if any(k in cleaned_name.lower() for k in skip_keywords):
            print(f"\nSkipping non-person entity: {author}")
            continue

        print(f"\nOriginal: {author}")
        print(f"Cleaned: {cleaned_name}")

        retries = 3

        while retries > 0:
            try:
                results = query_wikidata(cleaned_name)
                qid, birth = parse_result(results)

                if qid:
                    session.execute_write(update_author, author, qid, birth)
                    print(f"Updated {author} → {qid}")
                else:
                    print(f"No match found for {author} (searched as '{cleaned_name}')")

                break  # success, exit retry loop

            except Exception as e:
                print(f"Retrying {author} due to error: {e}")
                retries -= 1
                time.sleep(5)

        time.sleep(2)  # respect Wikidata rate limits

driver.close()