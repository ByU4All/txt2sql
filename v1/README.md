# Text→SQL Flow Diagram and Detailed Explanation

```mermaid
flowchart TD
  subgraph Offline[Offline Preparation]
    A1[Export schema: DDL + Light Markdown] --> A2[Sample cell values per column]
    A2 --> A3[Index cells into vector DB]
    A3 --> A4[Index example QA→SQL pairs]
    A4 --> A5[Build schema compression / table groups]
  end

  subgraph Online[Online Query Pipeline]
    Q[User Question]
    Q --> U[Task Understanding]
    U --> R[Retrieval]
    R --> G1[ICL Generator]
    R --> G2[Reasoning Generator (optional)]
    G1 --> M[Merge Candidates]
    G2 --> M
    M --> F[Iterative Refinement]
    F --> E[Execute Candidates]
    E --> H[Group by Execution Result]
    H --> S[Selection]
    S --> O[Final SQL Output]
    H --> C[Column Exploration (trigger)]
    C --> R
    S --> L[Logging & Monitoring]
  end

  %% Decision points and triggers
  U -.->|low_confidence| C
  M -.->|invalid_sql| F
  F -.->|still_invalid| C
  S -.->|low_confidence or tie| C

  style Offline fill:#f9f,stroke:#333,stroke-width:1px
  style Online fill:#9f9,stroke:#333,stroke-width:1px
```

---

# Summary

This document maps a production-ready Text→SQL pipeline that combines the reliability techniques from ReFoRCE with the multi-generator, selection and execution-based scoring from Agentar-Scale-SQL. The diagram above shows the high-level flow. Below are the components, decision points, and implementation details.

## 1. Offline Preparation (One-time or infrequent)

### 1.1 Export Schema

* Produce two schema views:

  * Full DDL view. Exact table definitions, types, and constraints.
  * Light Markdown view. Human-readable short forms, e.g., `orders(id:int, customer_id:int, amount:float)`.

### 1.2 Sample Cell Values

* For each column store top-N frequent/distinct values and some representative rows.
* Store typed tokens for numeric/date formats.

### 1.3 Index Cells and Examples

* Create two vector collections:

  * Cells collection: column-level values and short row snippets.
  * Examples collection: labeled QA→SQL pairs for retrieval in ICL.
* Use compact fast embeddings for cells (all-MiniLM or similar). Use stronger embeddings for examples if budget permits.

### 1.4 Schema Compression and Table Grouping

* For large DB: cluster related tables into logical groups. Create table-level summaries. This reduces retrieval noise and guides generators.

---

## 2. Online Pipeline (per query)

### 2.1 Task Understanding

* Normalize the user question (lowercase, canonicalize dates/numbers), extract keywords and a skeleton natural-language intent.
* Produce short "skeleton" used for example retrieval and for building light-schema prompts.
* Signals produced: `confidence_score` (how many schema tokens matched), `skeleton`

### 2.2 Retrieval

* Query both vectorstores:

  * Cells: retrieve candidate values used to ground WHERE clauses.
  * Examples: retrieve top-K QA→SQL pairs similar to the skeleton.
* K suggestions: cells K=20, examples K=4–8.

### 2.3 Candidate Generation (Parallel)

* ICL Generator (cheap, high recall):

  * Prompt includes: light schema, retrieved examples, retrieved values, and the question.
  * Generate multiple candidates by varying: prompt style, temperature, and few-shot combinations.

* Reasoning Generator (optional, high precision):

  * Use DDL-full schema plus an intrinsic reasoner model. Options:

    * Fine-tuned 7–13B model trained on in-domain SQL (if available).
    * Chain-of-thought prompting or CoT-like decomposition on a capable LLM.
  * Purpose: recover structurally different plans that ICL might not propose.

* Budget: aim for 6–12 total candidates. Practical sweet-spot ≈8.

### 2.4 Merge Candidates

* Deduplicate by normalized SQL (white-space, canonical aliasing) and by semantic equivalence where possible (same execution result on a small sample).

### 2.5 Iterative Refinement

* Run a syntax fixer on candidates that fail DB parse.
* Optionally run a semantic revisor that attempts small edits guided by execution feedback (e.g., adjust WHERE value quoting, cast mismatches).
* If a candidate is still invalid or low-confidence, trigger Column Exploration.

### 2.6 Execute Candidates

* Execute each candidate with a short timeout.
* Capture: execution_success, error message, rows_returned, execution_time, sample rows (LIMIT 5).

### 2.7 Group by Execution Result

* Group candidate SQLs by their execution outcomes and returned result set hash.
* This produces clusters that represent semantically-equivalent outputs.

### 2.8 Selection

* Simple production selector heuristics (fast):

  1. Prefer execution_success == True.
  2. Prefer clusters with more members (self-consistency).
  3. Prefer lower-complexity SQL within winning cluster (fewer joins/aggregates).
  4. Prefer queries that reference retrieved cell-values.

* Learned selector (better accuracy, slightly higher cost):

  * Train an XGBoost or small transformer to score candidates. Features include execution_success, rows_returned, group_size, where_values_match_ratio, num_joins, num_aggs, elapsed_time.
  * Train pairwise ranking or use tournament selection to pick winner.

* RL selector (highest research-grade accuracy):

  * Implement GRPO-style selector and train reward on EX metrics.

### 2.9 Column Exploration (Feedback Loop)

* Triggered by: low confidence, tie in selection, many invalid candidates, or ambiguous user question.
* Issue up to 8 exploratory SELECT queries (LIMIT small) to inspect candidate columns and sample values.
* Use returned samples to augment the prompt and rerun candidate generation.

### 2.10 Final Output and Logging

* Return final SQL and short execution summary: success, rows returned, any warnings.
* Log feature vectors for selector training and offline analysis.

---

## 3. Decision Points and Triggers

* **Skip reasoning generator** when latency or cost constraints exist. Rely on ICL + retrieval.
* **Trigger column exploration** only when selector confidence < threshold or tie occurs.
* **Cap candidate count** per query to control cost. 6–8 for low latency, 8–12 for balanced.

---

## 4. Selector Feature Schema (for XGBoost)

* `execution_success` boolean
* `rows_returned` integer
* `group_size` integer
* `where_values_match_ratio` float (0..1) — fraction of WHERE literals present among retrieved cell-values
* `num_joins` integer
* `num_aggregates` integer
* `sql_length` integer (chars)
* `execution_time_ms` integer
* `parse_error_flag` boolean
* `plan_cost_estimate` optional float
* `self_consistency_score` integer (members in same result cluster)

Label: `correct` (binary) from evaluation set.

---

## 5. Implementation Notes and APIs

* Vector DB: Chroma, Weaviate, or FAISS. Persist embeddings and metadata.
* Embedding models: all-MiniLM-L6-v2 for cell values; larger SBERT or OpenAI embeddings for examples if budget allows.
* LLMs: choose per budget. Options:

  * Local Llama2/GPT-J families for offline fine-tuning or intrinsic reasoner.
  * Hosted LLMs (GPT-4o/claude) for ICL if latency/cost acceptable.
* Executor: Use a DB user with read-only, limited-time queries to avoid heavy operations during execution. Always run queries in transaction/with EXPLAIN limits where possible.

---

## 6. Risks and Mitigations

* Hallucinated columns or tables: use schema compression and strict schema matching at prompt time. Reject queries referencing unknown tables unless value match exists.
* Cost and latency: cap candidates, disable reasoning generator for low-latency tier, use heuristic selector.
* Privacy: mask and restrict sample rows in logs. Anonymize sensitive cell-values when storing embeddings.

---

## 7. Next steps you can ask me to produce

* Export the mermaid diagram as PNG/SVG.
* A detailed component-level implementation (service diagrams, API endpoints, message formats).
* The XGBoost training script and dataset schema.
* Full prompt bank with 6 ICL styles and calibrated temperatures.

End of document.
