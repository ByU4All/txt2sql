import json
import asyncio
import logging
from src.schema.extraction.extractor import extract_postgres_schema, schema_to_dict, DatabaseSchema

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_schema_extraction(save_json_path=None):
    """Test the PostgreSQL schema extractor."""
    try:
        logger.info("🚀 Starting schema extraction...")

        # Extract schema from your test database
        schema = await extract_postgres_schema(['public'])

        # Print summary
        print("=" * 60)
        print("📊 DATABASE SCHEMA SUMMARY")
        print("=" * 60)
        print(f"Total Tables: {schema.total_tables}")
        print(f"Total Views: {schema.total_views}")
        print(f"Total Relationships: {len(schema.relationships)}")

        # Print tables
        if schema.tables:
            print("\n📋 TABLES:")
            print("-" * 40)
            for table_name, table_info in schema.tables.items():
                print(f"\n🗂️  {table_name}")
                print(f"   Type: {table_info.table_type}")
                print(f"   Columns: {len(table_info.columns)}")
                print(f"   Primary Keys: {table_info.primary_keys}")
                print(f"   Row Count: {table_info.row_count or 'Unknown'}")

                # Show column details
                print("   📄 Columns:")
                for col in table_info.columns:
                    pk_marker = " 🔑" if col.is_primary_key else ""
                    fk_marker = " 🔗" if col.is_foreign_key else ""
                    nullable = "NULL" if col.is_nullable else "NOT NULL"

                    print(f"      • {col.name}: {col.data_type} ({nullable}){pk_marker}{fk_marker}")

                    if col.is_foreign_key and col.foreign_table:
                        print(f"        └─ References: {col.foreign_table}.{col.foreign_column}")

                # Show indexes
                if table_info.indexes:
                    print("   🗂️  Indexes:")
                    for idx in table_info.indexes:
                        unique_marker = " (UNIQUE)" if idx.get('is_unique') else ""
                        primary_marker = " (PRIMARY)" if idx.get('is_primary') else ""
                        columns = ', '.join(idx.get('columns', []))
                        print(f"      • {idx.get('index_name')}: ({columns}){unique_marker}{primary_marker}")

        # Print views
        if schema.views:
            print("\n👁️  VIEWS:")
            print("-" * 40)
            for view_name, view_info in schema.views.items():
                print(f"\n📊 {view_name}")
                print(f"   Columns: {len(view_info.columns)}")

                # Show view columns
                for col in view_info.columns:
                    print(f"      • {col.name}: {col.data_type}")

        # Print relationships
        if schema.relationships:
            print("\n🔗 FOREIGN KEY RELATIONSHIPS:")
            print("-" * 40)
            for rel in schema.relationships:
                source = f"{rel['source_schema']}.{rel['source_table']}.{rel['source_column']}"
                target = f"{rel['target_schema']}.{rel['target_table']}.{rel['target_column']}"
                print(f"   {source} → {target}")

        print("\n" + "=" * 60)
        print("✅ Schema extraction completed successfully!")

        # Optional: Convert to dict and show structure
        schema_dict = schema_to_dict(schema)
        print(f"\n📋 Schema Dictionary Keys: {list(schema_dict.keys())}")

        if save_json_path:
            with open(save_json_path, "w") as f:
                json.dump(schema_dict, f, indent=2)
            print(f"📝 Schema saved to {save_json_path}")

        return schema

    except Exception as e:
        logger.error(f"❌ Schema extraction failed: {e}")
        raise


async def main():
    """Main function."""
    print("🔌 Testing PostgreSQL Schema Extractor")
    print("Database: postgresql://luke:password@localhost:5431/test_db")
    print("-" * 60)

    try:
        schema: DatabaseSchema = await test_schema_extraction(save_json_path="../../../data/schema_cache/schema_output.json")

        print(f"\n🎉 Test completed! Found {schema.total_tables} tables and {schema.total_views} views.")
    except Exception as e:
        print(f"\n💥 Test failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
