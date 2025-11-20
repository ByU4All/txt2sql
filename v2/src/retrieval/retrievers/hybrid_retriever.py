import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from pymilvus import Collection, connections, utility, MilvusClient
from neo4j import GraphDatabase
import numpy as np
import time

logger = logging.getLogger(__name__)


@dataclass
class RetrievalResult:
    """Container for retrieval results"""
    tables: List[Dict[str, Any]]
    columns: List[Dict[str, Any]]
    sample_values: List[Dict[str, Any]]
    joins: List[Dict[str, Any]]
    subgraph: Dict[str, Any]


class HybridRetriever:
    """
    Hybrid retriever that combines semantic retrieval from Milvus
    with structural traversal from Neo4j to build comprehensive schema subgraphs.
    """

    def __init__(
            self,
            milvus_host: str = "localhost",
            milvus_port: int = 19530,
            neo4j_uri: str = "bolt://localhost:7687",
            neo4j_user: str = "neo4j",
            neo4j_password: str = "password"
    ):
        self.milvus_host = milvus_host
        self.milvus_port = milvus_port
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password

        # Initialize connections
        self._connect_milvus()
        self._connect_neo4j()

        # Collection references
        self.tables_collection = None
        self.columns_collection = None
        self.cells_collection = None

    def _connect_milvus(self):
        """Connect to Milvus database"""
        try:
            connections.connect("default", host=self.milvus_host, port=str(self.milvus_port))
            logger.info("Connected to Milvus")
        except Exception as e:
            logger.error(f"Failed to connect to Milvus: {e}")
            raise

    def _connect_neo4j(self):
        """Connect to Neo4j database"""
        try:
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_uri,
                auth=(self.neo4j_user, self.neo4j_password)
            )
            # Test connection
            with self.neo4j_driver.session() as session:
                session.run("RETURN 1")
            logger.info("Connected to Neo4j")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise

    def initialize_collections(
            self,
            tables_collection_name: str = "tables",
            columns_collection_name: str = "columns",
            cells_collection_name: str = "cells",
            create_if_missing: bool = False
    ):
        """Initialize Milvus collections with improved loading"""
        try:
            # Check which collections exist
            existing_collections = utility.list_collections()
            logger.info(f"Available collections: {existing_collections}")

            collection_status = {
                "tables": {"exists": False, "loaded": False, "collection": None},
                "columns": {"exists": False, "loaded": False, "collection": None},
                "cells": {"exists": False, "loaded": False, "collection": None}
            }

            collections_to_process = [
                (tables_collection_name, "tables"),
                (columns_collection_name, "columns"),
                (cells_collection_name, "cells")
            ]

            for collection_name, key in collections_to_process:
                if collection_name in existing_collections:
                    collection_status[key]["exists"] = True
                    try:
                        # Create collection object first
                        collection_obj = Collection(collection_name)

                        # Use improved loading method
                        self._ensure_collection_loaded(collection_obj, collection_name, timeout=30)

                        # Assign to instance variable
                        setattr(self, f"{key}_collection", collection_obj)
                        collection_status[key]["loaded"] = True
                        collection_status[key]["collection"] = collection_obj

                        # Get entity count for verification
                        entity_count = collection_obj.num_entities
                        logger.info(f"✅ Collection '{collection_name}' loaded with {entity_count} entities")

                    except Exception as e:
                        logger.warning(f"⚠️ Failed to load collection '{collection_name}': {e}")
                else:
                    logger.warning(f"⚠️ Collection '{collection_name}' does not exist")

            # Summary report
            loaded_count = sum(1 for status in collection_status.values() if status["loaded"])
            total_expected = len(collection_status)

            logger.info("=== Collection Status Summary ===")
            for name, status in collection_status.items():
                if status["loaded"]:
                    entity_count = status["collection"].num_entities if status["collection"] else 0
                    logger.info(f"  ✅ {name}: Ready for retrieval ({entity_count} entities)")
                elif status["exists"]:
                    logger.info(f"  ⚠️ {name}: Exists but failed to load")
                else:
                    logger.info(f"  ❌ {name}: Collection missing")

            if loaded_count == 0:
                logger.error("❌ No collections are available for retrieval!")
                logger.info("📝 To fix this, try:")
                logger.info("   1. Run: python src/retrieval/reset_and_load_collections.py")
                logger.info("   2. If that fails, run: python src/schema/milvus_ingest.py")
                logger.info("   3. Then retry this test")
                return False
            elif loaded_count < total_expected:
                logger.warning(f"⚠️ Only {loaded_count}/{total_expected} collections loaded")
                logger.info("📝 Some collections failed to load. Try running reset script first.")
            else:
                logger.info(f"✅ All {loaded_count} collections ready for retrieval")

            return loaded_count > 0

        except Exception as e:
            logger.error(f"Failed to initialize collections: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _ensure_collection_loaded(self, collection, collection_name: str, timeout: int = 30):
        """Ensure collection is loaded using MilvusClient approach"""
        try:
            client = MilvusClient(uri=f"http://{self.milvus_host}:{self.milvus_port}")

            # Check current state
            load_state = client.get_load_state(collection_name=collection_name)
            logger.info(f"Collection '{collection_name}' current state: {load_state}")

            # If already loaded, we're done
            if load_state['state'].name == 'Loaded':
                logger.info(f"✅ Collection '{collection_name}' already loaded")
                return

            # If stuck in loading or we need to refresh, release first
            if load_state['state'].name in ['Loading', 'Loaded']:
                logger.info(f"Releasing collection '{collection_name}'...")
                client.release_collection(collection_name=collection_name)
                time.sleep(2)

            # Load the collection
            logger.info(f"Loading collection '{collection_name}'...")
            client.load_collection(collection_name=collection_name)

            # Wait for loading to complete
            start_time = time.time()
            while time.time() - start_time < timeout:
                load_state = client.get_load_state(collection_name=collection_name)

                if load_state['state'].name == 'Loaded':
                    logger.info(f"✅ Collection '{collection_name}' loaded successfully!")
                    return
                elif load_state['state'].name == 'Loading':
                    progress = load_state.get('progress', 'unknown')
                    logger.info(f"Loading '{collection_name}': {progress}%")
                    time.sleep(3)
                else:
                    logger.warning(f"Unexpected state for '{collection_name}': {load_state}")
                    time.sleep(3)

            raise TimeoutError(f"Collection '{collection_name}' failed to load within {timeout} seconds")

        except Exception as e:
            logger.error(f"Failed to ensure collection '{collection_name}' is loaded: {e}")
            raise

    def semantic_search(
            self,
            query_embedding: np.ndarray,
            top_k_tables: int = 5,
            top_n_columns: int = 10,
            top_m_values: int = 15
    ) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Perform semantic search across all collections"""

        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        tables, columns, sample_values = [], [], []

        # Search tables
        if self.tables_collection:
            try:
                results = self.tables_collection.search(
                    data=[query_embedding.tolist()],
                    anns_field="embedding",
                    param=search_params,
                    limit=top_k_tables,
                    output_fields=["table_name", "schema_name", "description"]
                )

                for hits in results:
                    for hit in hits:
                        tables.append({
                            "table_name": hit.entity.get("table_name", ""),
                            "schema_name": hit.entity.get("schema_name", "public"),
                            "description": hit.entity.get("description", ""),
                            "score": float(hit.score)
                        })

            except Exception as e:
                logger.warning(f"Table search failed: {e}")

        # Search columns
        if self.columns_collection:
            try:
                results = self.columns_collection.search(
                    data=[query_embedding.tolist()],
                    anns_field="embedding",
                    param=search_params,
                    limit=top_n_columns,
                    output_fields=["table_name", "column_name", "data_type", "description"]
                )

                for hits in results:
                    for hit in hits:
                        columns.append({
                            "table_name": hit.entity.get("table_name", ""),
                            "column_name": hit.entity.get("column_name", ""),
                            "data_type": hit.entity.get("data_type", ""),
                            "description": hit.entity.get("description", ""),
                            "score": float(hit.score)
                        })

            except Exception as e:
                logger.warning(f"Column search failed: {e}")

        # Search cell values
        if self.cells_collection:
            try:
                results = self.cells_collection.search(
                    data=[query_embedding.tolist()],
                    anns_field="embedding",
                    param=search_params,
                    limit=top_m_values,
                    output_fields=["table_name", "column_name", "cell_value"]
                )

                for hits in results:
                    for hit in hits:
                        sample_values.append({
                            "table_name": hit.entity.get("table_name", ""),
                            "column_name": hit.entity.get("column_name", ""),
                            "cell_value": hit.entity.get("cell_value", ""),
                            "score": float(hit.score)
                        })

            except Exception as e:
                logger.warning(f"Cell search failed: {e}")

        logger.info(f"Retrieved {len(tables)} tables, {len(columns)} columns, {len(sample_values)} values")
        return tables, columns, sample_values

    def get_joins_between_tables(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Get join relationships between tables matching your Neo4j schema"""
        joins = []

        with self.neo4j_driver.session() as session:
            try:
                direct_joins_query = """
                MATCH (t1:Table)-[r:REFERENCES]->(t2:Table)
                WHERE t1.name IN $table_names AND t2.name IN $table_names
                RETURN t1.name as table1, t1.schema as schema1,
                       t2.name as table2, t2.schema as schema2,
                       r.source_column as from_column,
                       r.target_column as to_column,
                       r.constraint_name as constraint_name
                """

                result = session.run(direct_joins_query, table_names=table_names)

                for record in result:
                    from_table = record['table1']
                    to_table = record['table2']
                    from_col = record['from_column']
                    to_col = record['to_column']

                    join_info = {
                        "from": f"{from_table}.{from_col}" if from_col else from_table,
                        "to": f"{to_table}.{to_col}" if to_col else to_table,
                        "table1": from_table,
                        "table2": to_table,
                        "from_column": from_col or '',
                        "to_column": to_col or '',
                        "constraint_name": record.get('constraint_name', ''),
                        "relationship_type": 'REFERENCES',
                        "path_length": 1
                    }
                    joins.append(join_info)

                logger.info(f"Found {len(joins)} direct REFERENCES relationships")

            except Exception as e:
                logger.warning(f"Join query failed: {e}")

            # Fallback: Try column-level FOREIGN_KEY_TO relationships
            if not joins:
                try:
                    column_joins_query = """
                    MATCH (c1:Column)-[r:FOREIGN_KEY_TO]->(c2:Column)
                    WHERE any(table IN $table_names WHERE c1.name STARTS WITH table + '.')
                    AND any(table IN $table_names WHERE c2.name STARTS WITH table + '.')
                    RETURN c1.name as from_column_full, c2.name as to_column_full
                    """

                    result = session.run(column_joins_query, table_names=table_names)

                    for record in result:
                        from_full = record['from_column_full']
                        to_full = record['to_column_full']

                        # Parse table.column format
                        from_parts = from_full.split('.', 1)
                        to_parts = to_full.split('.', 1)

                        if len(from_parts) == 2 and len(to_parts) == 2:
                            from_table, from_col = from_parts
                            to_table, to_col = to_parts

                            join_info = {
                                "from": f"{from_table}.{from_col}",
                                "to": f"{to_table}.{to_col}",
                                "table1": from_table,
                                "table2": to_table,
                                "from_column": from_col,
                                "to_column": to_col,
                                "constraint_name": '',
                                "relationship_type": 'FOREIGN_KEY_TO',
                                "path_length": 1
                            }
                            joins.append(join_info)

                    logger.info(f"Found {len(joins)} column-level foreign key relationships")

                except Exception as e:
                    logger.warning(f"Column join query failed: {e}")

        logger.info(f"Total joins found: {len(joins)}")
        return joins

    def find_join_paths(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Find join paths between tables including multi-hop relationships"""
        joins = []

        with self.neo4j_driver.session() as session:
            # Find direct joins first
            direct_joins = self.get_joins_between_tables(table_names)
            joins.extend(direct_joins)

            # Find multi-hop paths if no direct joins found
            if not joins:
                try:
                    multi_hop_query = """
                    MATCH path = (t1:Table)-[:REFERENCES*1..3]-(t2:Table)
                    WHERE t1.name IN $table_names AND t2.name IN $table_names
                    AND t1.name <> t2.name
                    AND length(path) > 1
                    RETURN t1.name as table1, t2.name as table2,
                           [node in nodes(path) | node.name] as path_tables,
                           length(path) as path_length
                    ORDER BY length(path)
                    LIMIT 50
                    """

                    result = session.run(multi_hop_query, table_names=table_names)

                    for record in result:
                        join_info = {
                            "from": f"{record['table1']}",
                            "to": f"{record['table2']}",
                            "table1": record['table1'],
                            "table2": record['table2'],
                            "from_column": '',
                            "to_column": '',
                            "constraint_name": '',
                            "relationship_type": 'MULTI_HOP',
                            "path_length": record['path_length'],
                            "path_tables": record['path_tables']
                        }
                        joins.append(join_info)

                    logger.info(f"Found {len(joins)} multi-hop join paths")

                except Exception as e:
                    logger.warning(f"Multi-hop join search failed: {e}")

        logger.info(f"Found {len(joins)} join paths total")
        return joins

    def add_missing_linking_tables(self, table_names: List[str], joins: List[Dict[str, Any]]) -> List[str]:
        """Add tables that connect the primary tables but weren't in initial results"""
        linking_tables = set()

        # Extract linking tables from multi-hop paths
        for join in joins:
            if join.get('path_length', 1) > 1 and 'path_tables' in join:
                path_tables = join['path_tables']
                # Add intermediate tables (exclude start and end)
                for table in path_tables[1:-1]:
                    if table not in table_names:
                        linking_tables.add(table)

        linking_tables_list = list(linking_tables)
        if linking_tables_list:
            logger.info(f"Added {len(linking_tables_list)} linking tables: {linking_tables_list}")

        return table_names + linking_tables_list

    def build_subgraph(self, table_names: List[str], joins: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build subgraph representation"""

        # Create nodes for tables
        tables_dict = {}
        for table_name in table_names:
            tables_dict[table_name] = {
                "name": table_name,
                "type": "table"
            }

        # Create edges for joins
        edges = []
        for join in joins:
            edge = {
                "from": join.get('table1', join.get('from', '').split('.')[0]),
                "to": join.get('table2', join.get('to', '').split('.')[0]),
                "relationship_type": join.get('relationship_type', 'UNKNOWN'),
                "source_column": join.get('from_column', ''),
                "target_column": join.get('to_column', ''),
                "path_length": join.get('path_length', 1)
            }
            edges.append(edge)

        subgraph = {
            "tables": list(tables_dict.values()),
            "joins": edges,
            "node_count": len(tables_dict),
            "edge_count": len(edges)
        }

        logger.info(f"Built subgraph with {len(tables_dict)} tables and {len(edges)} joins")
        return subgraph

    def retrieve(
            self,
            query_text: str,
            query_embedding: np.ndarray,
            top_k_tables: int = 5,
            top_n_columns: int = 10,
            top_m_values: int = 15
    ) -> dict:
        """Retrieve with exact output format requested including scores"""

        logger.info(f"Starting hybrid retrieval for: {query_text}")

        # Step 1: Semantic retrieval from Milvus
        tables, columns, values = self.semantic_search(
            query_embedding, top_k_tables, top_n_columns, top_m_values
        )

        # Step 2: Get table names for join search
        table_names = list(set([table['table_name'] for table in tables if table['table_name']]))

        # Step 3: Find joins and linking tables
        joins = self.find_join_paths(table_names)
        expanded_table_names = self.add_missing_linking_tables(table_names, joins)

        # Step 4: Build subgraph
        subgraph = self.build_subgraph(expanded_table_names, joins)

        # Step 5: Format output exactly as requested with scores
        result = {
            "tables": [
                {
                    "name": table['table_name'],
                    "score": float(table.get('score', 0))
                }
                for table in tables if table.get('table_name')
            ],

            "columns": [
                {
                    "name": f"{col.get('schema_name', 'public')}.{col['table_name']}.{col['column_name']}",
                    "score": float(col.get('score', 0))
                }
                for col in columns
                if col.get('table_name') and col.get('column_name')
            ],

            "joins": [
                {
                    "source_table": join.get('table1', join.get('from', '').split('.')[0]),
                    "target_table": join.get('table2', join.get('to', '').split('.')[0]),
                    "source_column": join.get('from_column', ''),
                    "target_column": join.get('to_column', '')
                }
                for join in joins
                if join.get('from_column') and join.get('to_column')
            ],

            "cell_values": [
                {
                    "value": str(val['cell_value']),
                    "score": float(val.get('score', 0))
                }
                for val in values
                if val.get('cell_value') is not None
            ]
        }

        logger.info("Hybrid retrieval completed successfully")
        logger.info(f"Retrieved {len(result['tables'])} tables, {len(result['columns'])} columns, "
                   f"{len(result['joins'])} joins, {len(result['cell_values'])} cell values")

        return result

    def close(self):
        """Close all connections"""
        if hasattr(self, 'neo4j_driver') and self.neo4j_driver:
            self.neo4j_driver.close()
            logger.info("Neo4j connection closed")

        try:
            connections.disconnect("default")
            logger.info("Milvus connection closed")
        except:
            pass

        logger.info("All connections closed")

