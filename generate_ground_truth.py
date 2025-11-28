

import json
import re
from typing import List, Dict, Tuple
from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE


class GroundTruthGenerator:
    """Generate ground truth from Neo4j Knowledge Graph."""
    
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        self.database = DATABASE
        self._check_connection()
    
    def _check_connection(self):
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (b:Book) RETURN count(b) as books
            """).single()
            print(f"Connected to Neo4j: {result['books']} books")
    
    def close(self):
        self.driver.close()
    
    def parse_query(self, query: str) -> Tuple[List[str], str]:
        """
        Parse query for AND/OR logic with plural expansion.
        
        Examples:
            "magic AND dragons" -> (["magic", "magics", "dragon", "dragons"], "AND")
            "witches OR wizards" -> (["witch", "witches", "wizard", "wizards"], "OR")
        """
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
            keywords = [w.strip().lower() for w in query.split() if w.strip()]
            logic = "OR"
        
        # Expand with singular/plural variants
        expanded = []
        for kw in keywords:
            expanded.append(kw)
            if kw.endswith('s') and len(kw) > 3:
                expanded.append(kw[:-1])  # singular
            elif not kw.endswith('s'):
                expanded.append(kw + 's')  # plural
        
        return list(dict.fromkeys(expanded)), logic
    
    def get_ground_truth(self, query: str, limit: int = 20) -> List[str]:
        """
        Find books matching query with AND/OR logic.
        Two-stage: Cypher CONTAINS narrows down, Python word boundary filters.
        """
        keywords, logic = self.parse_query(query)
        
        if not keywords:
            return []
        
        with self.driver.session(database=self.database) as session:
            # Stage 1: Cypher CONTAINS to narrow down
            result = session.run("""
                MATCH (b:Book)
                OPTIONAL MATCH (b)-[:HAS_SUBJECT]->(g:Genre)
                WITH b, collect(g.subject_1) AS subjects,
                     toLower(b.title) AS title_lower,
                     [s IN collect(g.subject_1) | toLower(coalesce(s, ''))] AS subjects_lower
                WHERE ANY(keyword IN $keywords WHERE 
                    title_lower CONTAINS keyword OR
                    ANY(s IN subjects_lower WHERE s CONTAINS keyword)
                )
                RETURN b.title AS title, subjects
                LIMIT 500
            """, keywords=keywords).data()
            
            # Stage 2: Python word boundary filtering
            matches = []
            for record in result:
                title = record['title'] or ''
                subjects = [s for s in record['subjects'] if s]
                
                search_text = title.lower()
                for s in subjects:
                    search_text += ' ' + s.lower()
                
                match_count = 0
                for kw in keywords:
                    pattern = r'\b' + re.escape(kw) + r'\b'
                    if re.search(pattern, search_text):
                        match_count += 1
                
                if logic == "AND" and match_count < len(keywords):
                    continue
                if logic == "OR" and match_count == 0:
                    continue
                
                matches.append(title)
            
            return matches[:limit]
    
    def generate_test_set(self) -> List[Dict]:
        """Generate test queries with ground truth from KG."""
        
        # Mixed AND/OR queries with proper keywords
        test_queries = [
            # AND queries - must match all keywords
            "magic AND fiction",
            "dragon AND adventure",
            "witch AND magic",
            "time travel AND fiction",
            "war AND history",
            
            # OR queries - match any keyword
            "witch OR wizard",
            "dragon OR magic",
            "king OR queen OR kingdom",
            "adventure OR quest",
            "fantasy OR fiction",
            
            # Simple queries (default OR)
            "juvenile fiction",
            "romance fiction",
        ]
        
        test_set = []
        
        print("\nGenerating ground truth from Neo4j:")
        print("  AND = must match ALL keywords")
        print("  OR = must match ANY keyword\n")
        
        for query in test_queries:
            keywords, logic = self.parse_query(query)
            ground_truth = self.get_ground_truth(query)
            
            entry = {
                "query": query,
                "query_type": "topic",
                "keywords": keywords,
                "logic": logic,
                "ground_truth": ground_truth,
                "num_ground_truth": len(ground_truth)
            }
            
            test_set.append(entry)
            print(f"  [{logic}] {query}: {len(ground_truth)} books")
            if ground_truth[:2]:
                for gt in ground_truth[:2]:
                    print(f"        {gt[:50]}...")
        
        valid = [t for t in test_set if t['num_ground_truth'] > 0]
        print(f"\nTotal: {len(valid)} queries with ground truth")
        
        return valid
    
    def save_test_set(self, filename: str = "test_set.json"):
        test_set = self.generate_test_set()
        with open(filename, 'w') as f:
            json.dump(test_set, f, indent=2)
        print(f"Saved to {filename}")
        return test_set


if __name__ == "__main__":
    generator = GroundTruthGenerator()
    generator.save_test_set("test_set.json")
    generator.close()