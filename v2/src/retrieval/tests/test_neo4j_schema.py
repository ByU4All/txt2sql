from neo4j import GraphDatabase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_neo4j_actual_schema():
    """Test what's actually in your Neo4j from graph_loader.py"""

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    with driver.session() as session:
        # Check what node types exist
        result = session.run("CALL db.labels()")
        labels = [record["label"] for record in result]
        logger.info(f"Available node labels: {labels}")

        # Check relationship types
        result = session.run("CALL db.relationshipTypes()")
        rel_types = [record["relationshipType"] for record in result]
        logger.info(f"Available relationship types: {rel_types}")

        # Check Table nodes
        result = session.run("MATCH (t:Table) RETURN t.name as name, t.schema as schema LIMIT 10")
        tables = [(record["name"], record["schema"]) for record in result]
        logger.info(f"Sample tables: {tables}")

        # Check REFERENCES relationships
        result = session.run("""
            MATCH (t1:Table)-[r:REFERENCES]->(t2:Table) 
            RETURN t1.name as from_table, t2.name as to_table, 
                   r.source_column as from_col, r.target_column as to_col,
                   r.constraint_name as constraint
            LIMIT 10
        """)

        references = []
        for record in result:
            references.append({
                "from": f"{record['from_table']}.{record['from_col']}",
                "to": f"{record['to_table']}.{record['to_col']}",
                "constraint": record['constraint']
            })

        logger.info(f"Sample REFERENCES relationships: {references}")

        # Check Column nodes
        result = session.run("MATCH (c:Column) RETURN c.name as name LIMIT 10")
        columns = [record["name"] for record in result]
        logger.info(f"Sample columns: {columns}")

    driver.close()


if __name__ == "__main__":
    test_neo4j_actual_schema()
