import asyncio
import logging
from typing import Dict, List, Optional, Any
from dataclasses import asdict
from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession
from neo4j.exceptions import ServiceUnavailable, ClientError

from .extractor import DatabaseSchema, TableInfo, ColumnInfo, extract_postgres_schema
# from ..config.settings import settings

logger = logging.getLogger(__name__)


class Neo4jSchemaLoader:
    """Loads PostgreSQL schema information into Neo4j graph database."""

    def __init__(self, neo4j_uri: Optional[str] = None,
                 neo4j_user: Optional[str] = None,
                 neo4j_password: Optional[str] = None):
        """Initialize Neo4j connection parameters."""
        # self.neo4j_uri = neo4j_uri or settings.database.neo4j_uri
        # self.neo4j_user = neo4j_user or settings.database.neo4j_user
        # self.neo4j_password = neo4j_password or settings.database.neo4j_password
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.driver: Optional[AsyncDriver] = None

    async def connect(self) -> None:
        """Establish connection to Neo4j."""
        try:
            self.driver = AsyncGraphDatabase.driver(
                self.neo4j_uri,
                auth=(self.neo4j_user, self.neo4j_password)
            )
            # Test the connection
            await self.driver.verify_connectivity()
            logger.info("✅ Connected to Neo4j database")
        except ServiceUnavailable as e:
            logger.error(f"❌ Failed to connect to Neo4j: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error connecting to Neo4j: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Neo4j connection."""
        if self.driver:
            await self.driver.close()
            logger.info("🔌 Disconnected from Neo4j database")

    async def clear_existing_schema(self) -> None:
        """Clear existing schema nodes and relationships."""
        if not self.driver:
            await self.connect()

        async with self.driver.session() as session:
            # Delete all schema-related nodes and relationships
            queries = [
                "MATCH (n:Database) DETACH DELETE n",
                "MATCH (n:Schema) DETACH DELETE n",
                "MATCH (n:Table) DETACH DELETE n",
                "MATCH (n:View) DETACH DELETE n",
                "MATCH (n:Column) DETACH DELETE n",
                "MATCH (n:Index) DETACH DELETE n"
            ]

            for query in queries:
                await session.run(query)

        logger.info("🧹 Cleared existing schema from Neo4j")

    async def load_schema(self, schema: DatabaseSchema,
                          database_name: str = "postgres",
                          clear_existing: bool = True) -> None:
        """Load complete database schema into Neo4j."""
        if not self.driver:
            await self.connect()

        if clear_existing:
            await self.clear_existing_schema()

        async with self.driver.session() as session:
            # Create database node
            await self._create_database_node(session, database_name, schema)

            # Create schema nodes (like 'public')
            schema_names = set()
            for table_info in list(schema.tables.values()) + list(schema.views.values()):
                schema_names.add(table_info.schema)

            for schema_name in schema_names:
                await self._create_schema_node(session, database_name, schema_name)

            # Create table nodes
            for table_name, table_info in schema.tables.items():
                await self._create_table_node(session, database_name, table_info)

            # Create view nodes
            for view_name, view_info in schema.views.items():
                await self._create_view_node(session, database_name, view_info)

            # Create column nodes and relationships
            for table_info in list(schema.tables.values()) + list(schema.views.values()):
                await self._create_columns(session, database_name, table_info)

            # Create index nodes
            for table_info in schema.tables.values():
                await self._create_indexes(session, database_name, table_info)

            # Create foreign key relationships
            await self._create_foreign_key_relationships(session, database_name, schema)

        logger.info("✅ Successfully loaded schema into Neo4j")

    async def _create_database_node(self, session: AsyncSession,
                                    database_name: str, schema: DatabaseSchema) -> None:
        """Create database node."""
        query = """
        CREATE (db:Database {
            name: $database_name,
            total_tables: $total_tables,
            total_views: $total_views,
            total_relationships: $total_relationships,
            created_at: datetime()
        })
        """
        await session.run(query, {
            'database_name': database_name,
            'total_tables': schema.total_tables,
            'total_views': schema.total_views,
            'total_relationships': len(schema.relationships)
        })

    async def _create_schema_node(self, session: AsyncSession,
                                  database_name: str, schema_name: str) -> None:
        """Create schema node and link to database."""
        query = """
        MATCH (db:Database {name: $database_name})
        CREATE (s:Schema {
            name: $schema_name,
            full_name: $database_name + '.' + $schema_name
        })
        CREATE (db)-[:CONTAINS_SCHEMA]->(s)
        """
        await session.run(query, {
            'database_name': database_name,
            'schema_name': schema_name
        })

    async def _create_table_node(self, session: AsyncSession,
                                 database_name: str, table_info: TableInfo) -> None:
        """Create table node and relationships."""
        query = """
        MATCH (s:Schema {name: $schema_name})
        CREATE (t:Table {
            name: $table_name,
            schema: $schema_name,
            full_name: $schema_name + '.' + $table_name,
            table_type: $table_type,
            column_count: $column_count,
            primary_keys: $primary_keys,
            row_count: $row_count,
            description: $description
        })
        CREATE (s)-[:CONTAINS_TABLE]->(t)
        """
        await session.run(query, {
            'schema_name': table_info.schema,
            'table_name': table_info.name,
            'table_type': table_info.table_type,
            'column_count': len(table_info.columns),
            'primary_keys': table_info.primary_keys,
            'row_count': table_info.row_count,
            'description': table_info.description
        })

    async def _create_view_node(self, session: AsyncSession,
                                database_name: str, view_info: TableInfo) -> None:
        """Create view node and relationships."""
        query = """
        MATCH (s:Schema {name: $schema_name})
        CREATE (v:View {
            name: $view_name,
            schema: $schema_name,
            full_name: $schema_name + '.' + $view_name,
            table_type: $table_type,
            column_count: $column_count
        })
        CREATE (s)-[:CONTAINS_VIEW]->(v)
        """
        await session.run(query, {
            'schema_name': view_info.schema,
            'view_name': view_info.name,
            'table_type': view_info.table_type,
            'column_count': len(view_info.columns)
        })

    async def _create_columns(self, session: AsyncSession,
                              database_name: str, table_info: TableInfo) -> None:
        """Create column nodes and relationships."""
        table_label = "Table" if table_info.table_type == "BASE TABLE" else "View"

        for i, column in enumerate(table_info.columns):
            query = f"""
            MATCH (t:{table_label} {{name: $table_name, schema: $schema_name}})
            CREATE (c:Column {{
                name: $column_name,
                data_type: $data_type,
                is_nullable: $is_nullable,
                default_value: $default_value,
                max_length: $max_length,
                numeric_precision: $numeric_precision,
                numeric_scale: $numeric_scale,
                is_primary_key: $is_primary_key,
                is_foreign_key: $is_foreign_key,
                foreign_table: $foreign_table,
                foreign_column: $foreign_column,
                ordinal_position: $ordinal_position
            }})
            CREATE (t)-[:HAS_COLUMN]->(c)
            """

            await session.run(query, {
                'table_name': table_info.name,
                'schema_name': table_info.schema,
                'column_name': column.name,
                'data_type': column.data_type,
                'is_nullable': column.is_nullable,
                'default_value': column.default_value,
                'max_length': column.max_length,
                'numeric_precision': column.numeric_precision,
                'numeric_scale': column.numeric_scale,
                'is_primary_key': column.is_primary_key,
                'is_foreign_key': column.is_foreign_key,
                'foreign_table': column.foreign_table,
                'foreign_column': column.foreign_column,
                'ordinal_position': i + 1
            })

    async def _create_indexes(self, session: AsyncSession,
                              database_name: str, table_info: TableInfo) -> None:
        """Create index nodes and relationships."""
        for index in table_info.indexes:
            query = """
            MATCH (t:Table {name: $table_name, schema: $schema_name})
            CREATE (idx:Index {
                name: $index_name,
                columns: $columns,
                is_unique: $is_unique,
                is_primary: $is_primary
            })
            CREATE (t)-[:HAS_INDEX]->(idx)
            """

            await session.run(query, {
                'table_name': table_info.name,
                'schema_name': table_info.schema,
                'index_name': index.get('index_name'),
                'columns': index.get('columns', []),
                'is_unique': index.get('is_unique', False),
                'is_primary': index.get('is_primary', False)
            })

    async def _create_foreign_key_relationships(self, session: AsyncSession,
                                                database_name: str, schema: DatabaseSchema) -> None:
        """Create foreign key relationships between tables."""
        for relationship in schema.relationships:
            query = """
            MATCH (source_table:Table {name: $source_table, schema: $source_schema})
            MATCH (target_table:Table {name: $target_table, schema: $target_schema})
            MATCH (source_col:Column {name: $source_column})-[:HAS_COLUMN*0..1]-(source_table)
            MATCH (target_col:Column {name: $target_column})-[:HAS_COLUMN*0..1]-(target_table)
            CREATE (source_table)-[:REFERENCES {
                constraint_name: $constraint_name,
                source_column: $source_column,
                target_column: $target_column
            }]->(target_table)
            CREATE (source_col)-[:FOREIGN_KEY_TO]->(target_col)
            """

            await session.run(query, {
                'source_table': relationship['source_table'],
                'source_schema': relationship['source_schema'],
                'target_table': relationship['target_table'],
                'target_schema': relationship['target_schema'],
                'source_column': relationship['source_column'],
                'target_column': relationship['target_column'],
                'constraint_name': relationship['constraint_name']
            })

    async def get_schema_summary(self) -> Dict[str, Any]:
        """Get summary of loaded schema from Neo4j."""
        if not self.driver:
            await self.connect()

        async with self.driver.session() as session:
            # Get counts
            queries = {
                'databases': "MATCH (n:Database) RETURN count(n) as count",
                'schemas': "MATCH (n:Schema) RETURN count(n) as count",
                'tables': "MATCH (n:Table) RETURN count(n) as count",
                'views': "MATCH (n:View) RETURN count(n) as count",
                'columns': "MATCH (n:Column) RETURN count(n) as count",
                'indexes': "MATCH (n:Index) RETURN count(n) as count",
                'relationships': "MATCH ()-[r:REFERENCES]->() RETURN count(r) as count"
            }

            summary = {}
            for key, query in queries.items():
                result = await session.run(query)
                record = await result.single()
                summary[key] = record['count'] if record else 0

            return summary


# Convenience functions
async def load_postgres_to_neo4j(
    postgres_schemas: Optional[List[str]] = None,
    neo4j_uri: Optional[str] = None,
    neo4j_user: Optional[str] = None,
    neo4j_password: Optional[str] = None,
    database_name: str = "postgres",
    clear_existing: bool = True
) -> Dict[str, Any]:
    """Complete pipeline: Extract PostgreSQL schema and load into Neo4j."""

    # Extract PostgreSQL schema
    logger.info("🔍 Extracting PostgreSQL schema...")
    postgres_schema = await extract_postgres_schema(postgres_schemas or ['public'])

    # Load into Neo4j
    logger.info("📊 Loading schema into Neo4j...")
    loader = Neo4jSchemaLoader(neo4j_uri, neo4j_user, neo4j_password)

    try:
        await loader.load_schema(postgres_schema, database_name, clear_existing)
        summary = await loader.get_schema_summary()
        logger.info(f"✅ Loaded schema: {summary}")
        return summary
    finally:
        await loader.disconnect()


async def get_neo4j_schema_summary(neo4j_uri: Optional[str] = None,
                                   neo4j_user: Optional[str] = None,
                                   neo4j_password: Optional[str] = None) -> Dict[str, Any]:
    """Get summary of schema loaded in Neo4j."""
    loader = Neo4jSchemaLoader(neo4j_uri, neo4j_user, neo4j_password)
    try:
        return await loader.get_schema_summary()
    finally:
        await loader.disconnect()
