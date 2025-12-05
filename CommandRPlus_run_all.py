# run_all.py  -- MASTER script to run everything
import time
import json, csv, os
from pathlib import Path
from datetime import datetime
import pandas as pd

from config import COHERE_API_KEY, COHERE_MODEL
if not COHERE_API_KEY:
    raise RuntimeError("Set COHERE_API_KEY in environment")

# Import RAG modules
from CommandRPlusGraph import CommandRPlusGraph
from CommandRPlusHyDE import CommandRPlusHyDE
from CommandRPlusMultiQuery import CommandRPlusMultiQuery
from CommandRPlusSubGraph import CommandRPlusSubGraph
from CommandRPlusVector import CommandRPlusVector
from generate_ground_truth import GroundTruthGenerator

OUT_JSON = "experiment_all_results.json"
OUT_CSV = "experiment_all_results.csv"

# Utility: detection of "no result" textual reply
NO_RESULT_TOKENS = ["no result","no results","i couldn't find","not find any","couldn't find any",
                    "no books found","i don't have any information","i don't have any"]

def is_no_result_text(answer: str) -> bool:
    if not answer: return False
    a = answer.lower()
    return any(tok in a for tok in NO_RESULT_TOKENS)

def normalize(s):
    return (s or "").strip().lower()

# Metrics helpers
def hit_at_k_retrieval(retrieved_titles, ground_truth, k=5):
    # handle ground_truth "No result" case (string) or empty list
    if (isinstance(ground_truth, str) and ground_truth.strip().lower()=="no result") or (not ground_truth):
        return 1.0 if len(retrieved_titles)==0 else 0.0
    gt = set([normalize(x) for x in ground_truth])
    topk = [normalize(x) for x in retrieved_titles[:k]]
    return 1.0 if any(t in gt for t in topk) else 0.0

def mrr_retrieval(retrieved_titles, ground_truth):
    if (isinstance(ground_truth, str) and ground_truth.strip().lower()=="no result") or (not ground_truth):
        return 1.0 if len(retrieved_titles)==0 else 0.0
    gt = set([normalize(x) for x in ground_truth])
    for i,t in enumerate(retrieved_titles, start=1):
        if normalize(t) in gt:
            return 1.0 / i
    return 0.0

def precision_at_k(retrieved_titles, ground_truth, k=5):
    if (isinstance(ground_truth, str) and ground_truth.strip().lower()=="no result") or (not ground_truth):
        return 1.0 if len(retrieved_titles)==0 else 0.0
    gt = set([normalize(x) for x in ground_truth])
    topk = [normalize(x) for x in retrieved_titles[:k]]
    if not topk: return 0.0
    return sum(1 for t in topk if t in gt) / len(topk)

def run_experiment_for_system(system_name, system, retrieve_fn, generate_fn, test_set, max_queries=None):
    results=[]
    queries = test_set[:max_queries] if max_queries else test_set
    for i,entry in enumerate(queries, start=1):
        q = entry['query']
        gt = entry.get('ground_truth', []) or []
        print(f"[{i}/{len(queries)}] {system_name} -> {q}")

        # retrieval
        t0 = time.time()
        try:
            retrieved = retrieve_fn(q, limit=10)
        except Exception as e:
            print(" Retrieval error:", e)
            retrieved = []
        retrieval_time = time.time() - t0

        # normalize retrieved into list of dicts with title
        retrieved_titles = [ (r.get('title') if isinstance(r, dict) else r) for r in retrieved ]

        # generation
        t1 = time.time()
        try:
            context = system.format_context(retrieved)
        except Exception:
            context = ""
        try:
            answer = generate_fn(q, context, books=retrieved) if 'books' in generate_fn.__code__.co_varnames else generate_fn(q, context, retrieved)
        except Exception as e:
            # fallback: try simpler call
            try:
                answer = generate_fn(q, context)
            except Exception as e2:
                answer = f"[GEN ERROR] {e}"
        generation_time = time.time() - t1

        # metrics
        hit5 = hit_at_k_retrieval(retrieved_titles, gt, k=5)
        mrr = mrr_retrieval(retrieved_titles, gt)
        prec5 = precision_at_k(retrieved_titles, gt, k=5)

        # special: if ground truth expects "No result" and model answered "no result", count generation correctness
        gen_no = is_no_result_text(answer)
        if (isinstance(entry.get('ground_truth'), str) and entry['ground_truth'].strip().lower()=="no result") or (not entry.get('ground_truth')):
            # if retrieval empty and model says no result -> generation correctness considered
            if len(retrieved_titles)==0 and gen_no:
                # treat as perfect generation for answer_relevance/faithfulness placeholders
                answer_relevance = 1.0
                faithfulness = 1.0
            else:
                answer_relevance = 0.0
                faithfulness = 0.0
        else:
            # we don't have automatic judge; use simple string overlap as a proxy
            answer_relevance = 1.0 if any(normalize(gt_item) in normalize(answer) for gt_item in (gt if isinstance(gt,list) else [gt])) else 0.0
            faithfulness = 1.0 if any(normalize(token) in normalize(context) for token in normalize(answer).split()[:10]) else 0.0

        ctx_use = 0.0
        if context:
            ctx_tokens = set(normalize(context).split())
            ans_tokens = set(normalize(answer).split())
            if ctx_tokens:
                ctx_use = len(ans_tokens & ctx_tokens) / len(ctx_tokens)

        results.append({
            "timestamp": datetime.now().isoformat(),
            "rag_method": system_name,
            "query": q,
            "ground_truth": "; ".join(entry.get('ground_truth',[])) if entry.get('ground_truth') else "No result",
            "retrieved_count": len(retrieved_titles),
            "retrieved_titles": "; ".join(retrieved_titles[:10]),
            "answer": answer,
            "hit@5": hit5,
            "mrr": round(mrr,4),
            "precision@5": round(prec5,4),
            "faithfulness": round(faithfulness,4),
            "answer_relevance": round(answer_relevance,4),
            "context_utilization": round(ctx_use,4),
            "retrieval_time": round(retrieval_time,4),
            "generation_time": round(generation_time,4)
        })
    return results

def main():
    # ensure test set
    if not Path("test_set.json").exists():
        print("Generating test set...")
        g = GroundTruthGenerator()
        ts = g.generate_test_set()
        with open("test_set.json","w") as f: json.dump(ts,f,indent=2)
        g = None
    with open("test_set.json","r") as f:
        test_set = json.load(f)

    # instantiate systems
    print("Loading systems...")
    graph = CommandRPlusGraph()
    hyde = CommandRPlusHyDE()
    multi = CommandRPlusMultiQuery()
    sub = CommandRPlusSubGraph()
    vect = CommandRPlusVector()

    all_results=[]
    try:
        all_results += run_experiment_for_system("GraphRAG", graph, graph.retrieve_books_by_keyword, graph.generate_response, test_set, max_queries=20)
        all_results += run_experiment_for_system("HyDE", hyde, hyde.retrieve_books_by_hyde, hyde.generate_response, test_set, max_queries=20)
        all_results += run_experiment_for_system("SubGraph", sub, sub.retrieve_books_by_subgraph, sub.generate_response, test_set, max_queries=20)
        all_results += run_experiment_for_system("Vector", vect, vect.retrieve_books_by_vector, vect.generate_response, test_set, max_queries=20)
        all_results += run_experiment_for_system("MultiQuery", multi, multi.retrieve_books_by_multiquery, multi.generate_response, test_set, max_queries=20)
    finally:
        graph.close(); hyde.close(); multi.driver.close(); sub.close(); vect.driver.close()

    # save JSON & CSV
    with open(OUT_JSON,"w") as f:
        json.dump(all_results,f,indent=2)
    df = pd.DataFrame(all_results)
    if not df.empty:
        df.to_csv(OUT_CSV, index=False)
        print(f"Saved {OUT_JSON} and {OUT_CSV}")
    else:
        print("No results to save")

if __name__=="__main__":
    main()
