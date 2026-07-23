"""
rag_visualize.py

Standalone, one-off script — NOT part of the deployed service.
Run this locally (same machine you tested test_embed.py on, so your
gcloud ADC credentials are already set up) to generate slide-ready
visuals of the RAG retrieval this project actually performs:

  1. rag_similarity_heatmap.png
     Every SQL query x every Beam function, cosine similarity.
     Rows are queries, columns are functions. The brightest cell in
     each row is the function RAG picks as that query's consumer.

  2. rag_embedding_space.png
     A 2D projection (PCA, no extra dependencies) of every embedded
     SQL query and Beam function. Points that are semantically related
     land near each other — this is literally what "retrieval by
     meaning, not keywords" looks like in space.

  3. rag_retrieval_table.csv
     query -> best-matched function -> file -> similarity score, as
     a plain table for a slide or the printed console output.

Usage:
    cd grid_frequency_hackathon
    pip3 install matplotlib numpy --break-system-packages   # if needed
    python3 rag_visualize.py
"""
import os
import ast
import csv
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
from google import genai

PROJECT_ID = os.environ.get("PROJECT_ID", "project-ff7c2ef5-8d88-401a-b86")
LOCATION = os.environ.get("LOCATION", "us-central1")
EMBED_MODEL = "text-embedding-004"

REPO_ROOT = Path(__file__).parent
SQL_DIR = REPO_ROOT / "resources" / "sql"
BEAM_DIR = REPO_ROOT / "src" / "beam"

client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)


def embed(texts: list[str]) -> np.ndarray:
    result = client.models.embed_content(model=EMBED_MODEL, contents=texts)
    return np.array([e.values for e in result.embeddings])


def load_sql_files() -> dict[str, str]:
    return {p.name: p.read_text() for p in sorted(SQL_DIR.glob("*.sql"))}


def load_beam_functions() -> list[dict]:
    """One entry per function def (top-level and nested/helper),
    same extraction logic as main.py's build_function_index."""
    entries = []
    for p in sorted(BEAM_DIR.glob("*.py")):
        src = p.read_text()
        try:
            tree = ast.parse(src)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                snippet = ast.get_source_segment(src, node)
                if snippet and snippet.strip():
                    entries.append({"file": p.name, "function": node.name, "source": snippet})
    return entries


def main():
    print(f"Reading from: {REPO_ROOT}")
    print("Loading SQL files...")
    sql_files = load_sql_files()
    print(f"  {len(sql_files)} SQL files: {list(sql_files)}")

    print("Extracting Beam functions...")
    functions = load_beam_functions()
    print(f"  {len(functions)} functions across "
          f"{len(set(f['file'] for f in functions))} files")

    if not sql_files or not functions:
        raise SystemExit(
            f"\nNo SQL files or Beam functions found under {REPO_ROOT}.\n"
            f"Expected: {SQL_DIR} and {BEAM_DIR}\n"
            f"Run this script from inside the grid_frequency_hackathon repo, "
            f"not BQ-ENGINE-GUARDRAIL — that's where resources/sql and src/beam live."
        )

    print("Embedding SQL queries...")
    sql_names = list(sql_files.keys())
    sql_vecs = embed(list(sql_files.values()))

    print("Embedding Beam functions...")
    fn_labels = [f"{f['file']}::{f['function']}" for f in functions]
    fn_vecs = embed([f["source"] for f in functions])

    # --- Cosine similarity matrix: queries (rows) x functions (cols) ---
    def cosine_matrix(a, b):
        a_norm = a / np.linalg.norm(a, axis=1, keepdims=True)
        b_norm = b / np.linalg.norm(b, axis=1, keepdims=True)
        return a_norm @ b_norm.T

    sim = cosine_matrix(sql_vecs, fn_vecs)

    # --- Visual 1: heatmap ---
    fig, ax = plt.subplots(figsize=(max(8, len(fn_labels) * 0.9), max(4, len(sql_names) * 0.6)))
    im = ax.imshow(sim, cmap="viridis", aspect="auto", vmin=0, vmax=1)
    ax.set_xticks(range(len(fn_labels)))
    ax.set_xticklabels(fn_labels, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(sql_names)))
    ax.set_yticklabels(sql_names, fontsize=9)
    ax.set_title("RAG retrieval: SQL query × Beam function cosine similarity\n"
                 "(brightest cell per row = the function RAG picks as consumer)",
                 fontsize=10)
    for i in range(len(sql_names)):
        best_j = int(np.argmax(sim[i]))
        ax.add_patch(plt.Rectangle((best_j - 0.5, i - 0.5), 1, 1,
                                    fill=False, edgecolor="red", linewidth=2))
    fig.colorbar(im, ax=ax, label="cosine similarity")
    fig.tight_layout()
    fig.savefig(REPO_ROOT / "rag_similarity_heatmap.png", dpi=180)
    print("Saved rag_similarity_heatmap.png")

    # --- Visual 2: 2D projection of the embedding space (manual PCA) ---
    all_vecs = np.vstack([sql_vecs, fn_vecs])
    all_labels = [f"SQL: {n}" for n in sql_names] + [f"Beam: {l}" for l in fn_labels]
    all_groups = (["query"] * len(sql_names)) + [f["file"] for f in functions]

    centered = all_vecs - all_vecs.mean(axis=0, keepdims=True)
    _, _, vt = np.linalg.svd(centered, full_matrices=False)
    proj = centered @ vt[:2].T  # top-2 principal components

    fig2, ax2 = plt.subplots(figsize=(9, 7))
    unique_groups = sorted(set(all_groups))
    cmap = plt.get_cmap("tab10")
    for gi, group in enumerate(unique_groups):
        idx = [i for i, g in enumerate(all_groups) if g == group]
        marker = "*" if group == "query" else "o"
        size = 220 if group == "query" else 70
        ax2.scatter(proj[idx, 0], proj[idx, 1], label=group,
                    marker=marker, s=size, color=cmap(gi % 10),
                    edgecolors="black", linewidths=0.5)
    for i, label in enumerate(all_labels):
        short = label.split("::")[-1].split(": ")[-1]
        ax2.annotate(short, (proj[i, 0], proj[i, 1]), fontsize=7,
                    xytext=(4, 4), textcoords="offset points")
    ax2.set_title("Beam function + SQL query embeddings, projected to 2D (PCA)\n"
                  "Stars = SQL queries, circles = Beam functions, colored by source file",
                  fontsize=10)
    ax2.set_xlabel("PC1")
    ax2.set_ylabel("PC2")
    ax2.legend(fontsize=8, loc="best")
    fig2.tight_layout()
    fig2.savefig(REPO_ROOT / "rag_embedding_space.png", dpi=180)
    print("Saved rag_embedding_space.png")

    # --- Table: best match per query ---
    rows = []
    for i, qname in enumerate(sql_names):
        best_j = int(np.argmax(sim[i]))
        rows.append({
            "sql_query": qname,
            "matched_function": functions[best_j]["function"],
            "matched_file": functions[best_j]["file"],
            "similarity": round(float(sim[i, best_j]), 3),
        })

    with open(REPO_ROOT / "rag_retrieval_table.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["sql_query", "matched_function", "matched_file", "similarity"])
        writer.writeheader()
        writer.writerows(rows)
    print("Saved rag_retrieval_table.csv\n")

    print(f"{'SQL query':<38} {'matched function':<28} {'file':<28} {'sim':>6}")
    print("-" * 104)
    for r in rows:
        print(f"{r['sql_query']:<38} {r['matched_function']:<28} {r['matched_file']:<28} {r['similarity']:>6}")


if __name__ == "__main__":
    main()
