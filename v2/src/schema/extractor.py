import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import asyncpg
from asyncpg import Connection, Pool

# from ..config.settings import settings


logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    data_type: str
    is_nullable: bool
    default_value: Optional[str] = None
    max_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_table: Optional[str] = None
    foreign_column: Optional[str] = None


@dataclass
class TableInfo:
    """Information about a database table."""
    name: str
    schema: str
    table_type: str
    columns: List[ColumnInfo]
    primary_keys: List[str]
    foreign_keys: List[Dict[str, str]]
    indexes: List[Dict[str, Any]]
    row_count: Optional[int] = None
    description: Optional[str] = None


@dataclass
class DatabaseSchema:
    """Complete database schema information."""
    tables: Dict[str, TableInfo]
    views: Dict[str, TableInfo]
    relationships: List[Dict[str, str]]
    total_tables: int
    total_views: int


class PostgreSQLSchemaExtractor:
    """Extracts schema information from PostgreSQL database."""

    def __init__(self):
        self.pool: Pool | None = None

    async def connect(self) -> None:
        """Establish connection pool to PostgreSQL."""
        try:
            self.pool = await asyncpg.create_pool(
                host="localhost",
                port="5431",
                user="luke",
                password="luke_password",
                database="test_db",
                min_size=1,
                max_size=5
            )
            # self.pool = await asyncpg.create_pool(
            #     host=settings.database.db_host,
            #     port=settings.database.db_port,
            #     user=settings.database.db_user,
            #     password=settings.database.db_password,
            #     database=settings.database.db_name,
            #     min_size=1,
            #     max_size=5
            # )
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def disconnect(self) -> None:
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Disconnected from PostgreSQL database")

    async def extract_schema(self, schema_names: Optional[List[str]] = None) -> DatabaseSchema:
        """Extract complete schema information from PostgreSQL."""
        if not self.pool:
            await self.connect()

        schema_filter = schema_names or ['public']

        async with self.pool.acquire() as conn:
            # Extract tables and views
            tables = await self._extract_tables(conn, schema_filter)
            views = await self._extract_views(conn, schema_filter)

            # Extract relationships
            relationships = await self._extract_relationships(conn, schema_filter)

            return DatabaseSchema(
                tables=tables,
                views=views,
                relationships=relationships,
                total_tables=len(tables),
                total_views=len(views)
            )

    async def _extract_tables(self, conn: Connection, schema_names: List[str]) -> Dict[str, TableInfo]:
        """Extract table information."""
        schema_list = "', '".join(schema_names)

        query = f"""
        SELECT 
            t.table_schema,
            t.table_name,
            t.table_type,
            obj_description(c.oid) as table_comment
        FROM information_schema.tables t
        LEFT JOIN pg_class c ON c.relname = t.table_name
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
        WHERE t.table_schema IN ('{schema_list}')
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_schema, t.table_name
        """

        rows = await conn.fetch(query)
        tables = {}

        for row in rows:
            table_key = f"{row['table_schema']}.{row['table_name']}"

            # Extract columns
            columns = await self._extract_columns(conn, row['table_schema'], row['table_name'])

            # Extract primary keys
            primary_keys = await self._extract_primary_keys(conn, row['table_schema'], row['table_name'])

            # Extract foreign keys
            foreign_keys = await self._extract_foreign_keys(conn, row['table_schema'], row['table_name'])

            # Extract indexes
            indexes = await self._extract_indexes(conn, row['table_schema'], row['table_name'])

            # Get row count
            row_count = await self._get_table_row_count(conn, row['table_schema'], row['table_name'])

            tables[table_key] = TableInfo(
                name=row['table_name'],
                schema=row['table_schema'],
                table_type=row['table_type'],
                columns=columns,
                primary_keys=primary_keys,
                foreign_keys=foreign_keys,
                indexes=indexes,
                row_count=row_count,
                description=row['table_comment']
            )

        return tables

    async def _extract_views(self, conn: Connection, schema_names: List[str]) -> Dict[str, TableInfo]:
        """Extract view information."""
        schema_list = "', '".join(schema_names)

        query = f"""
        SELECT 
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema IN ('{schema_list}')
        AND table_type = 'VIEW'
        ORDER BY table_schema, table_name
        """

        rows = await conn.fetch(query)
        views = {}

        for row in rows:
            view_key = f"{row['table_schema']}.{row['table_name']}"

            # Extract columns for views
            columns = await self._extract_columns(conn, row['table_schema'], row['table_name'])

            views[view_key] = TableInfo(
                name=row['table_name'],
                schema=row['table_schema'],
                table_type=row['table_type'],
                columns=columns,
                primary_keys=[],
                foreign_keys=[],
                indexes=[]
            )

        return views

    async def _extract_columns(self, conn: Connection, schema_name: str, table_name: str) -> List[ColumnInfo]:
        """Extract column information for a table."""
        query = """
        SELECT 
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
            CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key,
            fk.foreign_table_schema,
            fk.foreign_table_name,
            fk.foreign_column_name
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT ku.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = $1 AND tc.table_name = $2
        ) pk ON c.column_name = pk.column_name
        LEFT JOIN (
            SELECT 
                ku.column_name,
                ccu.table_schema as foreign_table_schema,
                ccu.table_name as foreign_table_name,
                ccu.column_name as foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
            JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = $1 AND tc.table_name = $2
        ) fk ON c.column_name = fk.column_name
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
        """

        rows = await conn.fetch(query, schema_name, table_name)
        columns = []

        for row in rows:
            foreign_table = None
            if row['is_foreign_key'] and row['foreign_table_schema'] and row['foreign_table_name']:
                foreign_table = f"{row['foreign_table_schema']}.{row['foreign_table_name']}"

            columns.append(ColumnInfo(
                name=row['column_name'],
                data_type=row['data_type'],
                is_nullable=row['is_nullable'] == 'YES',
                default_value=row['column_default'],
                max_length=row['character_maximum_length'],
                numeric_precision=row['numeric_precision'],
                numeric_scale=row['numeric_scale'],
                is_primary_key=row['is_primary_key'],
                is_foreign_key=row['is_foreign_key'],
                foreign_table=foreign_table,
                foreign_column=row['foreign_column_name']
            ))

        return columns

    async def _extract_primary_keys(self, conn: Connection, schema_name: str, table_name: str) -> List[str]:
        """Extract primary key columns."""
        query = """
        SELECT ku.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = $1 AND tc.table_name = $2
        ORDER BY ku.ordinal_position
        """

        rows = await conn.fetch(query, schema_name, table_name)
        return [row['column_name'] for row in rows]

    async def _extract_foreign_keys(self, conn: Connection, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """Extract foreign key relationships."""
        query = """
        SELECT 
            tc.constraint_name,
            ku.column_name,
            ccu.table_schema as foreign_table_schema,
            ccu.table_name as foreign_table_name,
            ccu.column_name as foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
        JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = $1 AND tc.table_name = $2
        """

        rows = await conn.fetch(query, schema_name, table_name)
        return [dict(row) for row in rows]

    async def _extract_indexes(self, conn: Connection, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Extract index information."""
        query = """
        SELECT 
            i.relname as index_name,
            array_agg(a.attname ORDER BY c.ordinality) as columns,
            ix.indisunique as is_unique,
            ix.indisprimary as is_primary
        FROM pg_class t
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN unnest(ix.indkey) WITH ORDINALITY c(attnum, ordinality) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = c.attnum
        WHERE n.nspname = $1 AND t.relname = $2
        GROUP BY i.relname, ix.indisunique, ix.indisprimary
        """

        rows = await conn.fetch(query, schema_name, table_name)
        return [dict(row) for row in rows]

    async def _get_table_row_count(self, conn: Connection, schema_name: str, table_name: str) -> Optional[int]:
        """Get approximate row count for a table."""
        try:
            query = f'SELECT reltuples::bigint FROM pg_class WHERE relname = $1'
            result = await conn.fetchval(query, table_name)
            return int(result) if result else 0
        except Exception as e:
            logger.warning(f"Could not get row count for {schema_name}.{table_name}: {e}")
            return None

    async def _extract_relationships(self, conn: Connection, schema_names: List[str]) -> List[Dict[str, str]]:
        """Extract all foreign key relationships."""
        schema_list = "', '".join(schema_names)

        query = f"""
        SELECT 
            tc.table_schema as source_schema,
            tc.table_name as source_table,
            ku.column_name as source_column,
            ccu.table_schema as target_schema,
            ccu.table_name as target_table,
            ccu.column_name as target_column,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
        JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema IN ('{schema_list}')
        """

        rows = await conn.fetch(query)
        return [dict(row) for row in rows]


# Factory function to create and use the extractor
async def extract_postgres_schema(schema_names: Optional[List[str]] = None) -> DatabaseSchema:
    """Extract PostgreSQL schema and return structured information."""
    extractor = PostgreSQLSchemaExtractor()
    try:
        schema = await extractor.extract_schema(schema_names)
        return schema
    finally:
        await extractor.disconnect()


# Utility function to convert schema to dictionary
def schema_to_dict(schema: DatabaseSchema) -> Dict[str, Any]:
    """Convert schema dataclass to dictionary for serialization."""
    return asdict(schema)

