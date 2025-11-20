import json, os
from pathlib import Path
from datetime import datetime

INPUT = "/home/prakhar/LUKE_DEV/prakhar-luke-dev/txt2sql/v2/data/schema_cache/schema_output.json"
OUT_DIR = "/data/vector_docs"
Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------------
# Auto cell generation heuristics
# -------------------------------------------------------------------
def auto_cell_values(column_name: str, data_type: str):
    name = column_name.lower()
    dtype = data_type.lower()

    # TEXT-LIKE columns
    if dtype in ("character varying", "varchar", "text"):
        if "email" in name:
            return ["user@example.com", "test@email.com"]
        if "phone" in name:
            return ["9876543210", "+91-9876543210"]
        if "city" in name:
            return ["Delhi", "Mumbai", "Bangalore"]
        if "country" in name:
            return ["India", "USA"]
        if "name" in name:
            return ["John", "Alice"]
        if "status" in name:
            return ["active", "pending", "closed"]
        if "channel" in name:
            return ["email", "phone", "web"]
        if "notes" in name:
            return ["sample note", "follow-up required"]
        if "description" in name:
            return ["sample description", "detailed text"]
        # Default text
        return ["sample_text", "example_value"]

    # NUMERIC columns
    if dtype in ("integer", "int", "numeric"):
        return ["0", "10", "100"]

    # DATE columns
    if dtype in ("date", "timestamp", "timestamp without time zone"):
        return ["2020-01-01", "2021-05-10"]

    # BOOLEAN
    if dtype in ("boolean", "bool"):
        return ["true", "false"]

    # FALLBACK
    return ["example"]

# -------------------------------------------------------------------
# Table summary generator
# -------------------------------------------------------------------
def summarize_table(name, cols):
    col_list = ", ".join(cols[:4])
    return f"{name}: stores records related to {col_list}, and other associated fields."

# -------------------------------------------------------------------
# Column description generator
# -------------------------------------------------------------------
def generate_column_text(table, col):
    name = col["name"].lower()
    dtype = col["data_type"].lower()

    if name.endswith("_id"):
        return f"{table}.{col['name']} (identifier): unique ID referencing another entity."
    if "date" in name:
        return f"{table}.{col['name']} (date): date associated with the record."
    if "amount" in name or "price" in name or "total" in name or "discount" in name:
        return f"{table}.{col['name']} ({dtype}): numeric or monetary amount."
    if "score" in name:
        return f"{table}.{col['name']} (integer): score or rating value."
    if "minutes" in name:
        return f"{table}.{col['name']} (integer): time duration in minutes."
    if "status" in name:
        return f"{table}.{col['name']} (string): status field."
    if "name" in name and table != "products":
        return f"{table}.{col['name']} (string): name field."
    return f"{table}.{col['name']} ({dtype}): attribute of {table}."

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
with open(INPUT, "r") as f:
    schema = json.load(f)

tables_docs = []
columns_docs = []
cells_docs = []

for full_table_name, tinfo in schema.get("tables", {}).items():
    table = tinfo["name"]
    schema_name = tinfo.get("schema", "public")
    cols = [c["name"] for c in tinfo.get("columns", [])]

    # ---------- Table Document ----------
    tables_docs.append({
        "id": f"table:{schema_name}.{table}",
        "text": summarize_table(table, cols),
        "metadata": {
            "type": "table",
            "schema": schema_name,
            "table_name": table,
            "full_name": f"{schema_name}.{table}",
            "column_names": cols,
            "primary_keys": tinfo.get("primary_keys", []),
            "foreign_keys": tinfo.get("foreign_keys", []),
            "generated_at": datetime.utcnow().isoformat()
        }
    })

    # ---------- Columns + Auto Cells ----------
    for i, col in enumerate(tinfo.get("columns", []), start=1):
        col_full = f"{schema_name}.{table}.{col['name']}"
        dtype = col.get("data_type", "")

        # Column doc
        columns_docs.append({
            "id": f"column:{col_full}",
            "text": generate_column_text(table, col),
            "metadata": {
                "type": "column",
                "schema": schema_name,
                "table_name": table,
                "column_name": col["name"],
                "full_name": col_full,
                "data_type": dtype,
                "is_primary_key": col.get("is_primary_key", False),
                "is_foreign_key": col.get("is_foreign_key", False),
                "foreign_table": col.get("foreign_table"),
                "foreign_column": col.get("foreign_column"),
                "ordinal_position": i
            }
        })

        # Auto cell value docs (synthetic)
        auto_values = auto_cell_values(col["name"], dtype)
        for val in auto_values:
            cells_docs.append({
                "id": f"cell:{col_full}:{val}",
                "text": val,
                "metadata": {
                    "type": "cell_value",
                    "schema": schema_name,
                    "table_name": table,
                    "column_name": col["name"],
                    "full_name": col_full,
                    "value_type": dtype,
                    "source": "auto_generated"
                }
            })

# -------- Write output files --------
with open(os.path.join(OUT_DIR, "tables_embeddings.json"), "w") as f:
    json.dump(tables_docs, f, indent=2)

with open(os.path.join(OUT_DIR, "columns_embeddings.json"), "w") as f:
    json.dump(columns_docs, f, indent=2)

with open(os.path.join(OUT_DIR, "cells_candidates.json"), "w") as f:
    json.dump(cells_docs, f, indent=2)

print("✓ Done. Generated:")
print(f"  - {len(tables_docs)} table docs")
print(f"  - {len(columns_docs)} column docs")
print(f"  - {len(cells_docs)} auto cell-value docs")
print(f"Files saved to: {OUT_DIR}/")
