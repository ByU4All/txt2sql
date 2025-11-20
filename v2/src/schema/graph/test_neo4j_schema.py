from neo4j import GraphDatabase

# -------------------------------------------------------------
# CONFIGURE THESE
# -------------------------------------------------------------
NEO4J_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "password"
# -------------------------------------------------------------

driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))

# -------------------------------------------------------------
# Cypher Validation Queries
# -------------------------------------------------------------
QUERIES = {
    "count_nodes": """
        MATCH (n)
        RETURN labels(n) AS label, count(*) AS count
    """,

    "count_relationships": """
        MATCH ()-[r]->()
        RETURN type(r) AS relationship_type, count(*) AS count
    """,

    "tables_without_columns": """
        MATCH (t:Table)
        WHERE NOT (t)-[:HAS_COLUMN]->(:Column)
        RETURN t.name AS table_without_columns
    """,

    "columns_attached_to_multiple_tables": """
        MATCH (c:Column)<-[:HAS_COLUMN]-(t:Table)
        WITH c, count(t) AS table_count
        WHERE table_count > 1
        RETURN c.name AS column_name, table_count
    """,

    "duplicate_column_names": """
        MATCH (c:Column)
        WITH c.name AS name, count(*) AS cnt
        WHERE cnt > 1
        RETURN name, cnt
    """,

    "foreign_key_edges": """
        MATCH (c1:Column)-[:FOREIGN_KEY_TO]->(c2:Column)
        RETURN c1.name AS source_column, c2.name AS target_column
        ORDER BY source_column
    """,

    "table_level_fk_edges": """
        MATCH (t1:Table)-[r:REFERENCES]->(t2:Table)
        RETURN t1.name AS source_table, t2.name AS target_table,
               r.source_column AS fk_source_column,
               r.target_column AS fk_target_column
        ORDER BY source_table
    """,

    "validate_fk_column_mapping": """
        MATCH (src_tab:Table)-[:HAS_COLUMN]->(src_col:Column)-[:FOREIGN_KEY_TO]->(tgt_col:Column)
        MATCH (tgt_tab:Table)-[:HAS_COLUMN]->(tgt_col)
        RETURN src_tab.name AS source_table,
               src_col.name AS source_column,
               tgt_tab.name AS target_table,
               tgt_col.name AS target_column
        ORDER BY source_table
    """,

    "disconnected_tables": """
        MATCH (t:Table)
        WHERE NOT (t)-[:REFERENCES|FOREIGN_KEY_TO*1..3]-(:Table)
        RETURN t.name AS disconnected_table
    """,

    "all_join_paths_example_sales_customers": """
        MATCH p = allShortestPaths(
            (a:Table {name:'sales'})-[:REFERENCES|FOREIGN_KEY_TO*1..5]-(b:Table {name:'customers'})
        )
        RETURN p
    """,
}

# -------------------------------------------------------------
# Helper function to run queries
# -------------------------------------------------------------
def run_query(name, query):
    print(f"\n===== CHECK: {name} =====")
    with driver.session() as session:
        result = session.run(query)
        rows = list(result)

        if not rows:
            print("No results.\n")
        else:
            for row in rows:
                print(dict(row))


# -------------------------------------------------------------
# Main Validation Runner
# -------------------------------------------------------------
def main():
    print("Running Neo4j Schema Validation Suite...\n")

    for name, query in QUERIES.items():
        run_query(name, query)

    print("\n✔ Validation Complete.\n")


if __name__ == "__main__":
    main()
