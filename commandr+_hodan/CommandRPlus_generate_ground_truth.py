# generate_ground_truth.py
import json
import re
from typing import List, Tuple
from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE

class GroundTruthGenerator:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        self.database = DATABASE

    def close(self): self.driver.close()

    def parse_query(self, query: str) -> Tuple[list,str]:
        q = query.upper()
        if " AND " in q:
            parts = re.split(r'\s+AND\s+', query, flags=re.IGNORECASE)
            kws = [p.strip().lower() for p in parts if p.strip()]
            return list(dict.fromkeys(kws + [k+'s' for k in kws])), "AND"
        elif " OR " in q:
            parts = re.split(r'\s+OR\s+', query, flags=re.IGNORECASE)
            kws = [p.strip().lower() for p in parts if p.strip()]
            return list(dict.fromkeys(kws + [k+'s' for k in kws])), "OR"
        else:
            words = [w.strip().lower() for w in query.split() if w.strip()]
            return words, "OR"

    def get_ground_truth(self, query, limit=50):
        kws, logic = self.parse_query(query)
        if not kws: return []
        with self.driver.session(database=self.database) as s:
            rows = s.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WITH b, collect(g.subject_1) AS subjects, toLower(b.title) AS t, [s IN collect(g.subject_1) | toLower(coalesce(s,''))] AS subs
                WHERE ANY(k IN $kws WHERE t CONTAINS k OR ANY(s IN subs WHERE s CONTAINS k))
                RETURN b.title AS title, subjects LIMIT 500
            """, kws=kws).data()
        matches=[]
        for r in rows:
            title = r['title'] or ''
            subs = [s for s in r['subjects'] if s]
            text = title.lower() + " " + " ".join([s.lower() for s in subs])
            count = sum(1 for k in kws if re.search(r'\b'+re.escape(k)+r'\b', text))
            if logic=="AND" and count < len(kws): continue
            if logic=="OR" and count==0: continue
            matches.append(title)
        return matches[:limit]

    def generate_test_set(self):
        queries = [
            "magic AND fiction", "dragon AND adventure", "witch AND magic", "time travel AND fiction", "war AND history",
            "witch OR wizard", "dragon OR magic", "king OR queen OR kingdom", "adventure OR quest", "fantasy OR fiction",
            "juvenile fiction", "romance fiction"
        ]
        out=[]
        for q in queries:
            kws, logic = self.parse_query(q)
            gt = self.get_ground_truth(q)
            out.append({'query':q,'query_type':'topic','keywords':kws,'logic':logic,'ground_truth':gt,'num_ground_truth':len(gt)})
            print(f"[{logic}] {q}: {len(gt)}")
        valid=[t for t in out if t['num_ground_truth']>0]
        print(f"Total valid: {len(valid)}")
        return valid

if __name__=="__main__":
    g = GroundTruthGenerator()
    ts = g.generate_test_set()
    with open("test_set.json","w") as f:
        json.dump(ts,f,indent=2)
    g.close()
    print("Saved test_set.json")
