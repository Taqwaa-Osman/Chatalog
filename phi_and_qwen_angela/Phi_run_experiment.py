import time
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Set
from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE

class ExperimentRunner: 
    def __init__(self, test_set_file: str = "test_set.json"):
        self.neo4j_uri = NEO4J_URI
        self.neo4j_user = NEO4J_USER
        self.neo4j_password = NEO4J_PASSWORD
        self.database = DATABASE
        
        self.driver = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))
        
        self.test_set = self._load_test_set(test_set_file)
        self.results_file = Path('experiment_results.json')
        self.all_results = self._load_results()
    
    def _load_test_set(self, filename: str) -> List[Dict]:
        path = Path(filename)
        if path.exists():
            with open(path, 'r') as f:
                test_set = json.load(f)
            print(f"Loaded {len(test_set)} test queries from {filename}")
            return test_set
        else:
            print(f"No test set at {filename}. Run: python generate_ground_truth.py")
            return []
    
    def _load_results(self) -> List[Dict]:
        if self.results_file.exists():
            with open(self.results_file, 'r') as f:
                return json.load(f)
        return []
    
    def save_results(self):
        with open(self.results_file, 'w') as f:
            json.dump(self.all_results, f, indent=2)
    
    def close(self):
        self.driver.close()
    
    def load_rag_system(self, rag_name: str):
        """Load RAG system and return (system, core_retrieve_function)."""
        if rag_name == "graphrag":
            from PhiGraph import GraphRAGPhi3
            system = GraphRAGPhi3(self.neo4j_uri, self.neo4j_user, self.neo4j_password, self.database)
            return system, system.retrieve_books_by_keyword
        
        elif rag_name == "hyde":
            from PhiHYDE import HydeRAGPhi3
            system = HydeRAGPhi3(self.neo4j_uri, self.neo4j_user, self.neo4j_password, self.database)
            return system, system.retrieve_books_by_hyde
        
        elif rag_name == "subgraph":
            from PhiSubgraph import SubGraphRAGPhi3
            system = SubGraphRAGPhi3(self.neo4j_uri, self.neo4j_user, self.neo4j_password, self.database)
            return system, system.retrieve_books_by_subgraph
        
        elif rag_name == "vector":
            from PhiVector import VectorRAGPhi3
            system = VectorRAGPhi3(self.neo4j_uri, self.neo4j_user, self.neo4j_password, self.database)
            return system, system.retrieve_books_by_vector
        
        elif rag_name == "multiquery":
            from PhiMultiquery import MultiQueryRAGPhi3
            system = MultiQueryRAGPhi3(self.neo4j_uri, self.neo4j_user, self.neo4j_password, self.database)
            return system, system.retrieve_books_by_multiquery
        
        else:
            raise ValueError(f"Unknown RAG: {rag_name}")
    
    # Retrieval Metrics
    def calculate_hit_at_k(self, retrieved: List[str], relevant: Set[str], k: int = 5) -> float:
        return 1.0 if any(book in relevant for book in retrieved[:k]) else 0.0
    
    def calculate_mrr(self, retrieved: List[str], relevant: Set[str]) -> float:
        for idx, book in enumerate(retrieved, start=1):
            if book in relevant:
                return 1.0 / idx
        return 0.0
    
    def calculate_precision_at_k(self, retrieved: List[str], relevant: Set[str], k: int = 5) -> float:
        top_k = retrieved[:k]
        if not top_k:
            return 0.0
        return sum(1 for book in top_k if book in relevant) / len(top_k)
    
    def calculate_recall_at_k(self, retrieved: List[str], relevant: Set[str], k: int = 5) -> float:
        if not relevant:
            return 0.0
        top_k = set(retrieved[:k])
        return len(top_k & relevant) / len(relevant)
    
    # Generation Metrics (LLM-as-Judge)
    def judge_faithfulness(self, context: str, answer: str) -> float:
        import ollama
        prompt = f"""Rate 0.0 to 1.0: Is this answer grounded in the context without hallucination?

Context: {context[:800]}
Answer: {answer[:400]}

Reply with only a number:"""
        try:
            response = ollama.generate(model="phi3:mini", prompt=prompt, options={"temperature": 0.1})
            score = float(response['response'].strip().split()[0])
            return max(0.0, min(1.0, score))
        except:
            return 0.5
    
    def judge_answer_relevance(self, query: str, answer: str) -> float:
        import ollama
        prompt = f"""Rate 0.0 to 1.0: Does this answer address the question?

Question: {query}
Answer: {answer[:400]}

Reply with only a number:"""
        try:
            response = ollama.generate(model="phi3:mini", prompt=prompt, options={"temperature": 0.1})
            score = float(response['response'].strip().split()[0])
            return max(0.0, min(1.0, score))
        except:
            return 0.5
    
    def judge_context_relevance(self, query: str, context: str) -> float:
        import ollama
        prompt = f"""Rate 0.0 to 1.0: Is this context relevant to the question?

Question: {query}
Context: {context[:800]}

Reply with only a number:"""
        try:
            response = ollama.generate(model="phi3:mini", prompt=prompt, options={"temperature": 0.1})
            score = float(response['response'].strip().split()[0])
            return max(0.0, min(1.0, score))
        except:
            return 0.5
    
    def run_single_query(self, system, retrieve_func, generate_func, query: str, ground_truth: List[str]) -> Dict:
        # Retrieval
        retrieval_start = time.time()
        retrieved_books = retrieve_func(query, limit=10)
        retrieval_time = time.time() - retrieval_start
        
        context = system.format_context(retrieved_books)
        
        # Generation
        generation_start = time.time()
        answer = generate_func(query, context, books=retrieved_books, stream=False)
        generation_time = time.time() - generation_start
        
        # Metrics
        retrieved_titles = [book['title'] for book in retrieved_books]
        relevant_set = set(ground_truth)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "hit_at_5": self.calculate_hit_at_k(retrieved_titles, relevant_set, k=5),
            "mrr": self.calculate_mrr(retrieved_titles, relevant_set),
            "precision_at_5": self.calculate_precision_at_k(retrieved_titles, relevant_set, k=5),
            "recall_at_5": self.calculate_recall_at_k(retrieved_titles, relevant_set, k=5),
            "faithfulness": self.judge_faithfulness(context, answer),
            "answer_relevance": self.judge_answer_relevance(query, answer),
            "context_relevance": self.judge_context_relevance(query, context),
            "retrieval_time_sec": round(retrieval_time, 3),
            "generation_time_sec": round(generation_time, 3),
            "total_time_sec": round(retrieval_time + generation_time, 3),
            "num_retrieved": len(retrieved_books),
            "num_ground_truth": len(ground_truth),
            "retrieved_books": retrieved_titles,
            "ground_truth": ground_truth,
            "answer": answer[:500],
            "context": context[:1000]
        }
    
    def run_experiments(self, rag_name: str, max_queries: int = None):
        if not self.test_set:
            print("No test set. Run: python generate_ground_truth.py")
            return []
        
        print(f"\nExperiment: {rag_name.upper()}")
        print(f"Testing core retrieval method")
        
        system, retrieve_func = self.load_rag_system(rag_name)
        generate_func = system.generate_response
        
        queries_to_run = self.test_set[:max_queries] if max_queries else self.test_set
        print(f"Running {len(queries_to_run)} queries...\n")
        
        results = []
        
        for idx, test_query in enumerate(queries_to_run, 1):
            query = test_query['query']
            ground_truth = test_query.get('ground_truth', [])
            logic = test_query.get('logic', 'OR')
            
            print(f"[{idx}/{len(queries_to_run)}] [{logic}] {query}")
            
            if not ground_truth:
                print("  No ground truth, skipping")
                continue
            
            try:
                metrics = self.run_single_query(system, retrieve_func, generate_func, query, ground_truth)
                metrics['rag_method'] = rag_name
                metrics['llm_model'] = 'phi3:mini'
                metrics['query'] = query
                metrics['query_type'] = test_query.get('query_type', 'topic')
                metrics['logic'] = test_query.get('logic', 'OR')
                
                results.append(metrics)
                
                print(f"  Hit@5={metrics['hit_at_5']:.1f} MRR={metrics['mrr']:.2f} P@5={metrics['precision_at_5']:.2f}")
                
            except Exception as e:
                print(f"  Error: {e}")
                continue
        
        system.close()
        
        if results:
            self.all_results.extend(results)
            self.save_results()
            self._print_summary(rag_name, results)
        
        return results
    
    def _print_summary(self, rag_name: str, results: List[Dict]):
        df = pd.DataFrame(results)
        
        print(f"\nSummary: {rag_name.upper()}")
        print(f"Queries: {len(results)}")
        print(f"Retrieval - Hit@5: {df['hit_at_5'].mean():.3f}, MRR: {df['mrr'].mean():.3f}, P@5: {df['precision_at_5'].mean():.3f}")
        print(f"Generation - Faith: {df['faithfulness'].mean():.3f}, AnsRel: {df['answer_relevance'].mean():.3f}, CtxRel: {df['context_relevance'].mean():.3f}")
        print(f"Timing - Retrieval: {df['retrieval_time_sec'].mean():.2f}s, Generation: {df['generation_time_sec'].mean():.2f}s\n")
    
    def print_comparison(self):
        if not self.all_results:
            print("No results to compare")
            return
        
        df = pd.DataFrame(self.all_results)
        
        summary = df.groupby('rag_method').agg({
            'hit_at_5': 'mean',
            'mrr': 'mean',
            'precision_at_5': 'mean',
            'recall_at_5': 'mean',
            'faithfulness': 'mean',
            'answer_relevance': 'mean',
            'context_relevance': 'mean',
            'retrieval_time_sec': 'mean',
            'generation_time_sec': 'mean',
            'query': 'count'
        }).round(3)
        
        summary = summary.rename(columns={'query': 'num_queries'})
        
        print("\nRAG Method Comparison")
        print(summary.to_string())
        print()
        
        return summary
    
    def export_to_csv(self, filename: str = "experiment_results.csv"):
        if not self.all_results:
            return
        
        df = pd.DataFrame(self.all_results)
        df['retrieved_books'] = df['retrieved_books'].apply(lambda x: '; '.join(x[:5]))
        df['ground_truth'] = df['ground_truth'].apply(lambda x: '; '.join(x[:5]))
        df = df.drop(columns=['answer', 'context'], errors='ignore')
        df.to_csv(filename, index=False)
        print(f"Exported to {filename}")
    
    def clear_results(self):
        self.all_results = []
        self.save_results()


if __name__ == "__main__":
    runner = ExperimentRunner(test_set_file="test_set.json")
    
    try:
        if not runner.test_set:
            print("\nFirst generate a test set:")
            print("  python generate_ground_truth.py fantasy_books.csv\n")
        else:
            runner.clear_results()
            
            print("\nChatalog RAG Comparison Experiment")
            print("Testing 5 RAG methods\n")
            
            runner.run_experiments("graphrag", max_queries=10)
            runner.run_experiments("hyde", max_queries=10)
            runner.run_experiments("subgraph", max_queries=10)
            runner.run_experiments("vector", max_queries=10)
            runner.run_experiments("multiquery", max_queries=10)
            
            runner.print_comparison()
            runner.export_to_csv()
    
    finally:
        runner.close()