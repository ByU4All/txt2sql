import asyncio
import logging
from typing import Dict, Any

from src.schema.graph_loader import (
    Neo4jSchemaLoader,
    load_postgres_to_neo4j,
    get_neo4j_schema_summary
)
from src.schema.extractor import extract_postgres_schema

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_neo4j_connection():
    """Test Neo4j connection with manual credentials."""
    print("🔌 Testing Neo4j Connection")
    print("-" * 50)

    # Manual Neo4j connection details
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "your_password_here"  # Replace with your Neo4j password

    loader = Neo4jSchemaLoader(neo4j_uri, neo4j_user, neo4j_password)

    try:
        await loader.connect()
        print("✅ Successfully connected to Neo4j!")

        # Test basic query
        async with loader.driver.session() as session:
            result = await session.run("RETURN 'Hello Neo4j!' as message")
            record = await result.single()
            print(f"📝 Test query result: {record['message']}")

        return True

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    finally:
        await loader.disconnect()


async def test_complete_pipeline():
    """Test complete PostgreSQL to Neo4j pipeline."""
    print("\n" + "=" * 60)
    print("🚀 TESTING COMPLETE POSTGRES → NEO4J PIPELINE")
    print("=" * 60)

    # Connection details
    postgres_uri = "postgresql://luke:luke_password@localhost:5431/test_db"
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "your_password_here"  # Replace with your Neo4j password

    try:
        # Run the complete pipeline
        summary = await load_postgres_to_neo4j(
            database_uri=postgres_uri,
            postgres_schemas=['public'],
            neo4j_uri=neo4j_uri,
            neo4j_user=neo4j_user,
            neo4j_password=neo4j_password,
            database_name="test_db",
            clear_existing=True
        )

        print("\n📊 PIPELINE SUMMARY:")
        print("-" * 40)
        for key, value in summary.items():
            print(f"  {key.capitalize()}: {value}")

        return summary

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise


async def test_neo4j_queries():
    """Test various Neo4j queries on loaded schema."""
    print("\n" + "=" * 60)
    print("🔍 TESTING NEO4J SCHEMA QUERIES")
    print("=" * 60)

    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "your_password_here"  # Replace with your Neo4j password

    loader = Neo4jSchemaLoader(neo4j_uri, neo4j_user, neo4j_password)

    try:
        await loader.connect()

        async with loader.driver.session() as session:
            # Test queries
            queries = [
                {
                    'name': 'Database Overview',
                    'query': """
                    MATCH (db:Database)
                    RETURN db.name as database_name,
                           db.total_tables as tables,
                           db.total_views as views,
                           db.total_relationships as relationships
                    """
                },
                {
                    'name': 'All Tables',
                    'query': """
                    MATCH (t:Table)
                    RETURN t.name as table_name,
                           t.schema as schema_name,
                           t.column_count as columns,
                           t.row_count as row_count
                    ORDER BY t.name
                    """
                },
                {
                    'name': 'Table Relationships',
                    'query': """
                    MATCH (source:Table)-[r:REFERENCES]->(target:Table)
                    RETURN source.name as source_table,
                           target.name as target_table,
                           r.source_column as source_column,
                           r.target_column as target_column
                    """
                },
                {
                    'name': 'Primary Key Columns',
                    'query': """
                    MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
                    WHERE c.is_primary_key = true
                    RETURN t.name as table_name,
                           c.name as column_name,
                           c.data_type as data_type
                    ORDER BY t.name
                    """
                },
                {
                    'name': 'Foreign Key Relationships',
                    'query': """
                    MATCH (source_col:Column)-[:FOREIGN_KEY_TO]->(target_col:Column)
                    MATCH (source_table:Table)-[:HAS_COLUMN]->(source_col)
                    MATCH (target_table:Table)-[:HAS_COLUMN]->(target_col)
                    RETURN source_table.name as source_table,
                           source_col.name as source_column,
                           target_table.name as target_table,
                           target_col.name as target_column
                    """
                },
                {
                    'name': 'Column Data Types Summary',
                    'query': """
                    MATCH (c:Column)
                    RETURN c.data_type as data_type,
                           count(c) as column_count
                    ORDER BY column_count DESC
                    """
                }
            ]

            for query_info in queries:
                print(f"\n🔍 {query_info['name']}:")
                print("-" * 30)

                try:
                    result = await session.run(query_info['query'])
                    records = await result.data()

                    if records:
                        for i, record in enumerate(records[:5]):  # Show first 5 results
                            print(f"  {i + 1}. {dict(record)}")

                        if len(records) > 5:
                            print(f"  ... and {len(records) - 5} more records")
                    else:
                        print("  No results found")

                except Exception as e:
                    print(f"  ❌ Query failed: {e}")

    finally:
        await loader.disconnect()


async def test_schema_summary():
    """Test getting schema summary from Neo4j."""
    print("\n" + "=" * 60)
    print("📋 TESTING SCHEMA SUMMARY")
    print("=" * 60)

    try:
        summary = await get_neo4j_schema_summary(
            neo4j_uri="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="your_password_here"  # Replace with your Neo4j password
        )

        print("📊 Current Neo4j Schema Summary:")
        print("-" * 40)
        for key, value in summary.items():
            emoji = {
                'databases': '🗄️',
                'schemas': '📁',
                'tables': '🗂️',
                'views': '👁️',
                'columns': '📋',
                'indexes': '🗂️',
                'relationships': '🔗'
            }.get(key, '📌')

            print(f"  {emoji} {key.replace('_', ' ').title()}: {value}")

        return summary

    except Exception as e:
        print(f"❌ Failed to get schema summary: {e}")
        raise


async def test_step_by_step():
    """Test each step separately for debugging."""
    print("\n" + "=" * 60)
    print("🔧 TESTING STEP-BY-STEP")
    print("=" * 60)

    postgres_uri = "postgresql://luke:luke_password@localhost:5431/test_db"
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "your_password_here"  # Replace with your Neo4j password

    try:
        # Step 1: Extract PostgreSQL schema
        print("1️⃣ Extracting PostgreSQL Schema...")
        schema = await extract_postgres_schema(
            database_uri=postgres_uri,
            schema_names=['public']
        )
        print(f"   ✅ Found {schema.total_tables} tables, {schema.total_views} views")

        # Step 2: Connect to Neo4j
        print("\n2️⃣ Connecting to Neo4j...")
        loader = Neo4jSchemaLoader(neo4j_uri, neo4j_user, neo4j_password)
        await loader.connect()
        print("   ✅ Connected successfully")

        # Step 3: Clear existing data
        print("\n3️⃣ Clearing existing schema...")
        await loader.clear_existing_schema()
        print("   ✅ Cleared existing data")

        # Step 4: Load schema
        print("\n4️⃣ Loading schema into Neo4j...")
        await loader.load_schema(schema, "test_db", clear_existing=False)
        print("   ✅ Schema loaded successfully")

        # Step 5: Verify
        print("\n5️⃣ Verifying loaded data...")
        summary = await loader.get_schema_summary()
        print(f"   ✅ Verification complete: {summary}")

        await loader.disconnect()
        return True

    except Exception as e:
        print(f"❌ Step-by-step test failed: {e}")
        raise


async def main():
    """Run all tests."""
    print("🧪 TESTING NEO4J GRAPH LOADER")
    print("=" * 60)

    # Update this with your actual Neo4j password
    print("⚠️  Make sure to update the Neo4j password in this file!")
    print("   Default Neo4j URI: bolt://localhost:7687")
    print("   Default Neo4j User: neo4j")
    print("   Update neo4j_password variable in each test function")
    print()

    tests = [
        ("Neo4j Connection", test_neo4j_connection),
        ("Complete Pipeline", test_complete_pipeline),
        ("Schema Queries", test_neo4j_queries),
        ("Schema Summary", test_schema_summary),
        ("Step by Step", test_step_by_step)
    ]

    results = {}
    for test_name, test_func in tests:
        try:
            print(f"\n🧪 Running: {test_name}")
            result = await test_func()
            results[test_name] = "✅ PASSED"
            print(f"✅ {test_name} completed successfully!")
        except Exception as e:
            results[test_name] = f"❌ FAILED: {e}"
            print(f"❌ {test_name} failed: {e}")
            # Continue with other tests

    # Final summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    for test_name, result in results.items():
        print(f"  {result} {test_name}")


if __name__ == "__main__":
    asyncio.run(main())
