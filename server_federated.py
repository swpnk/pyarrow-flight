# federated_server.py
# Arrow Flight server that handles federated queries across multiple data sources
# including PostgreSQL and Delta Lake

import os
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import duckdb
import sqlalchemy
from sqlalchemy import create_engine, text
import argparse
import json
import concurrent.futures
import time
from typing import List, Dict, Any, Optional, Tuple
from pyarrow.flight import FlightServerBase, ServerAuthHandler, FlightDescriptor

try:
    import deltalake
    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False
    print("Delta Lake support not available. Install with: pip install deltalake")

class DataSource:
    """Represents a data source that can be queried."""
    
    def __init__(self, name: str, source_type: str, connection_info: Dict[str, Any]):
        self.name = name
        self.source_type = source_type
        self.connection_info = connection_info
        self.is_connected = False
        self.connection = None
        self.capabilities = self._determine_capabilities()
    
    def _determine_capabilities(self) -> Dict[str, bool]:
        """Determine the capabilities of this data source."""
        capabilities = {
            "pushdown_filters": False,
            "pushdown_aggregates": False,
            "pushdown_joins": False,
            "supports_transactions": False,
            "native_sql": False
        }
        
        # Set capabilities based on source type
        if self.source_type == "duckdb":
            capabilities.update({
                "pushdown_filters": True,
                "pushdown_aggregates": True,
                "pushdown_joins": True,
                "native_sql": True
            })
        elif self.source_type == "postgresql":
            capabilities.update({
                "pushdown_filters": True,
                "pushdown_aggregates": True,
                "pushdown_joins": True,
                "supports_transactions": True,
                "native_sql": True
            })
        elif self.source_type == "delta":
            capabilities.update({
                "pushdown_filters": True,
                "native_sql": False  # Delta Lake requires a compute engine
            })
        elif self.source_type == "flight":
            # Assuming remote Flight server has similar capabilities to DuckDB
            capabilities.update({
                "pushdown_filters": True,
                "pushdown_aggregates": True,
                "pushdown_joins": True,
                "native_sql": True
            })
        
        return capabilities
    
    def connect(self) -> bool:
        """Establish connection to the data source."""
        try:
            if self.source_type == "duckdb":
                # Connect to a local DuckDB database
                self.connection = duckdb.connect(database=self.connection_info.get("database", ":memory:"), 
                                               read_only=self.connection_info.get("read_only", False))
                
                # Handle Parquet files
                if "parquet_path" in self.connection_info:
                    parquet_path = self.connection_info["parquet_path"]
                    self.connection.execute(f"CREATE VIEW IF NOT EXISTS {self.name} AS SELECT * FROM parquet_scan('{parquet_path}')")
                
                # Handle CSV files
                elif "csv_path" in self.connection_info:
                    csv_path = self.connection_info["csv_path"]
                    self.connection.execute(f"CREATE VIEW IF NOT EXISTS {self.name} AS SELECT * FROM read_csv_auto('{csv_path}')")
                    
                self.is_connected = True
                return True
                
            elif self.source_type == "postgresql":
                # Connect to PostgreSQL database
                conn_string = self.connection_info.get("connection_string", "")
                if not conn_string:
                    # Build connection string from components
                    host = self.connection_info.get("host", "localhost")
                    port = self.connection_info.get("port", 5432)
                    database = self.connection_info.get("database", "")
                    user = self.connection_info.get("user", "")
                    password = self.connection_info.get("password", "")
                    
                    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
                
                self.connection = create_engine(conn_string)
                # Test connection
                with self.connection.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                self.is_connected = True
                return True
                
            elif self.source_type == "delta" and DELTA_AVAILABLE:
                # Connect to Delta Lake table
                delta_path = self.connection_info.get("path", "")
                if not delta_path:
                    print(f"No path specified for Delta Lake source: {self.name}")
                    return False
                
                # We'll initialize a DuckDB connection to query the Delta table
                self.connection = duckdb.connect(database=":memory:")
                
                # Load Delta table into DuckDB
                # Check if the httpfs extension is needed (for S3 paths)
                if delta_path.startswith("s3://"):
                    self.connection.execute("INSTALL httpfs; LOAD httpfs;")
                
                # Register the Delta table
                self.connection.execute(f"""
                    INSTALL delta;
                    LOAD delta;
                    CREATE VIEW {self.name} AS SELECT * FROM delta_scan('{delta_path}');
                """)
                
                self.is_connected = True
                return True
                
            elif self.source_type == "flight":
                # Connect to another Flight server
                location = self.connection_info.get("location", "")
                self.connection = flight.FlightClient(location)
                # Test connection by listing flights
                list(self.connection.list_flights())
                self.is_connected = True
                return True
                
            else:
                print(f"Unsupported data source type: {self.source_type}")
                return False
                
        except Exception as e:
            print(f"Error connecting to {self.source_type} source '{self.name}': {e}")
            return False
    
    def get_schema(self, table_name: Optional[str] = None) -> Optional[pa.Schema]:
        """Get the schema of a table or view in this data source."""
        if not self.is_connected and not self.connect():
            return None
            
        try:
            if self.source_type == "duckdb":
                # Use DuckDB to get schema
                if table_name is None:
                    table_name = self.name
                
                # Query with LIMIT 0 to get schema without data
                result = self.connection.execute(f"SELECT * FROM {table_name} LIMIT 0").arrow()
                return result.schema
                
            elif self.source_type == "postgresql":
                # Use SQLAlchemy to get column info
                if table_name is None:
                    # Use default schema/table from config
                    schema = self.connection_info.get("schema", "public")
                    table_name = self.connection_info.get("table", "")
                    if not table_name:
                        return None
                    
                    qualified_table = f"{schema}.{table_name}" if schema else table_name
                else:
                    qualified_table = table_name
                
                # Query with LIMIT 0 to get schema without data
                with self.connection.connect() as conn:
                    df = pd.read_sql(f"SELECT * FROM {qualified_table} LIMIT 0", conn)
                    return pa.Schema.from_pandas(df)
                    
            elif self.source_type == "delta" and DELTA_AVAILABLE:
                # Use DuckDB with Delta Lake integration
                if table_name is None:
                    table_name = self.name
                
                result = self.connection.execute(f"SELECT * FROM {table_name} LIMIT 0").arrow()
                return result.schema
                
            elif self.source_type == "flight":
                # For a Flight source, list flights and get schema from the first one
                # This is a simplification - in practice, you would need to query specific paths
                for flight_info in self.connection.list_flights():
                    return flight_info.schema
                
            return None
            
        except Exception as e:
            print(f"Error getting schema from {self.source_type} source '{self.name}': {e}")
            return None
    
    def execute_query(self, query: str) -> Optional[pa.Table]:
        """Execute a query on this data source and return results as Arrow table."""
        if not self.is_connected and not self.connect():
            return None
            
        try:
            if self.source_type == "duckdb":
                # Execute query in DuckDB
                start_time = time.time()
                result = self.connection.execute(query).arrow()
                end_time = time.time()
                print(f"DuckDB query executed in {end_time - start_time:.3f}s")
                return result
                
            elif self.source_type == "postgresql":
                # Execute query in PostgreSQL
                start_time = time.time()
                with self.connection.connect() as conn:
                    # Use pandas to read SQL and convert to Arrow
                    df = pd.read_sql(query, conn)
                    result = pa.Table.from_pandas(df)
                end_time = time.time()
                print(f"PostgreSQL query executed in {end_time - start_time:.3f}s")
                return result
                
            elif self.source_type == "delta" and DELTA_AVAILABLE:
                # Execute query using DuckDB with Delta integration
                start_time = time.time()
                result = self.connection.execute(query).arrow()
                end_time = time.time()
                print(f"Delta query executed in {end_time - start_time:.3f}s")
                return result
                
            elif self.source_type == "flight":
                # Execute query on remote Flight server
                start_time = time.time()
                descriptor = flight.FlightDescriptor.for_command(query.encode('utf-8'))
                flight_info = self.connection.get_flight_info(descriptor)
                
                if not flight_info.endpoints:
                    print(f"No endpoints returned for query on Flight source: {self.name}")
                    return None
                    
                endpoint = flight_info.endpoints[0]
                ticket = endpoint.ticket
                reader = self.connection.do_get(ticket)
                result = reader.read_all()
                end_time = time.time()
                print(f"Flight query executed in {end_time - start_time:.3f}s")
                return result
                
            return None
            
        except Exception as e:
            print(f"Error executing query on {self.source_type} source '{self.name}': {e}")
            print(f"Query: {query}")
            return None

class QueryPlan:
    """Represents a plan for executing a federated query."""
    
    def __init__(self, full_query: str):
        self.full_query = full_query
        self.source_queries = {}  # Map of source name to query
        self.result_processing = None  # Function to process results
        self.estimated_cost = 0
    
    def add_source_query(self, source_name: str, query: str, estimated_cost: float = 1.0):
        """Add a query to be executed on a specific source."""
        self.source_queries[source_name] = query
        self.estimated_cost += estimated_cost
    
    def set_result_processing(self, processing_function):
        """Set the function to process and combine results."""
        self.result_processing = processing_function
    
    def __str__(self):
        plan_str = f"Query Plan for: {self.full_query}\n"
        plan_str += f"Estimated cost: {self.estimated_cost}\n"
        plan_str += "Source queries:\n"
        for source, query in self.source_queries.items():
            plan_str += f"  {source}: {query}\n"
        return plan_str

class FederatedQueryPlanner:
    """Planner for federated queries."""
    
    def __init__(self, data_sources: Dict[str, DataSource]):
        self.data_sources = data_sources
    
    def create_query_plan(self, query: str) -> QueryPlan:
        """
        Create a plan for executing a federated query.
        
        This is a simplified query planner. In a real system, you would use
        a proper query optimizer like Apache Calcite.
        """
        plan = QueryPlan(query)
        
        # Simplistic parsing to find referenced sources
        referenced_sources = {}
        for source_name, source in self.data_sources.items():
            source_ref = f"{source_name}."
            if source_ref in query:
                referenced_sources[source_name] = source
        
        # If no sources explicitly referenced, check if it's a simple query
        # that could be executed on any source
        if not referenced_sources:
            # Look for table references
            tables_mentioned = self._extract_tables_from_query(query)
            
            if not tables_mentioned:
                # If no specific tables mentioned, try to find a source that can handle this query
                for source_name, source in self.data_sources.items():
                    if source.capabilities["native_sql"]:
                        plan.add_source_query(source_name, query)
                        break
            else:
                # Try to map mentioned tables to sources
                for table in tables_mentioned:
                    for source_name, source in self.data_sources.items():
                        # Check if this source has this table
                        # This is a simplification - in practice, you would check metadata
                        if source.get_schema(table) is not None:
                            referenced_sources[source_name] = source
                            break
        
        # If we found sources, create source-specific queries
        if referenced_sources:
            for source_name, source in referenced_sources.items():
                rewritten_query = self._rewrite_query_for_source(source_name, query)
                plan.add_source_query(source_name, rewritten_query)
            
            # Set result processing function - combine results
            if len(referenced_sources) > 1:
                plan.set_result_processing(self._combine_results)
            
        else:
            # Fallback: try to run the query on any source that supports SQL
            for source_name, source in self.data_sources.items():
                if source.capabilities["native_sql"]:
                    plan.add_source_query(source_name, query)
                    break
        
        return plan
    
    def _extract_tables_from_query(self, query: str) -> List[str]:
        """
        Extract table names from a SQL query.
        
        This is a simplified implementation. In a real system, you would
        use a proper SQL parser.
        """
        # Split the query into words
        words = query.replace(',', ' ').replace('(', ' ').replace(')', ' ').split()
        
        tables = []
        # Look for words after FROM and JOIN
        for i, word in enumerate(words):
            if word.upper() in ('FROM', 'JOIN') and i + 1 < len(words):
                table = words[i + 1]
                # Remove any aliases
                if ' AS ' in table:
                    table = table.split(' AS ')[0]
                tables.append(table)
        
        return tables
    
    def _rewrite_query_for_source(self, source_name: str, query: str) -> str:
        """
        Rewrite a query for a specific source.
        
        This is a simplified implementation. In a real system, you would
        parse the query and transform the AST.
        """
        # Replace references to the current source
        source_ref = f"{source_name}."
        query = query.replace(source_ref, "")
        
        # Remove references to other sources
        for other_source in self.data_sources:
            if other_source != source_name:
                other_ref = f"{other_source}."
                query = query.replace(other_ref, "")
        
        return query
    
    def _combine_results(self, results: Dict[str, pa.Table]) -> pa.Table:
        """
        Combine results from multiple sources.
        
        This is a simplified implementation. In a real system, you would
        implement proper join, union, and other operations.
        """
        tables = list(results.values())
        
        if not tables:
            return pa.Table.from_arrays([], [])
            
        if len(tables) == 1:
            return tables[0]
            
        # Try to concatenate tables - they need compatible schemas
        try:
            return pa.concat_tables(tables)
        except pa.ArrowInvalid as e:
            print(f"Error combining results: {e}")
            # Return the first table as a fallback
            return tables[0]

class FederatedQueryEngine:
    """Engine for executing federated queries."""
    
    def __init__(self, data_sources: Dict[str, DataSource]):
        self.data_sources = data_sources
        self.planner = FederatedQueryPlanner(data_sources)
    
    def execute_query(self, query: str) -> Optional[pa.Table]:
        """
        Execute a federated query and return results.
        """
        # Create a query plan
        plan = self.planner.create_query_plan(query)
        print(f"Executing query plan:\n{plan}")
        
        # Execute queries on each source
        results = {}
        
        # Use parallel execution for better performance
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(plan.source_queries)) as executor:
            futures = {}
            
            for source_name, source_query in plan.source_queries.items():
                source = self.data_sources.get(source_name)
                if source:
                    future = executor.submit(source.execute_query, source_query)
                    futures[future] = source_name
            
            for future in concurrent.futures.as_completed(futures):
                source_name = futures[future]
                try:
                    result = future.result()
                    if result and len(result) > 0:
                        results[source_name] = result
                except Exception as e:
                    print(f"Error executing query on {source_name}: {e}")
        
        # Combine results if needed
        if plan.result_processing:
            return plan.result_processing(results)
        elif results:
            # Return the first result if no processing function
            return next(iter(results.values()))
        else:
            # No results
            return pa.Table.from_arrays([], [])
    
    def get_query_schema(self, query: str) -> Optional[pa.Schema]:
        """
        Determine the schema of a query result without executing the full query.
        """
        # Create a query plan
        plan = self.planner.create_query_plan(query)
        
        # Try to get schema from one of the sources
        for source_name, source_query in plan.source_queries.items():
            source = self.data_sources.get(source_name)
            if source:
                try:
                    # Add a LIMIT 0 to the query to get just the schema
                    schema_query = f"SELECT * FROM ({source_query}) AS subquery LIMIT 0"
                    result = source.execute_query(schema_query)
                    if result:
                        return result.schema
                except Exception as e:
                    print(f"Error getting schema from {source_name}: {e}")
        
        # Fallback: create a minimal schema
        return pa.schema([])
    
    def estimate_query_size(self, query: str) -> Tuple[int, int]:
        """
        Estimate the number of rows and size of the query result.
        """
        # Create a query plan
        plan = self.planner.create_query_plan(query)
        
        total_rows = 0
        total_bytes = 0
        
        # Try to get estimates from each source
        for source_name, source_query in plan.source_queries.items():
            source = self.data_sources.get(source_name)
            if source:
                try:
                    # Try to get count
                    count_query = f"SELECT COUNT(*) FROM ({source_query}) AS subquery"
                    count_result = source.execute_query(count_query)
                    if count_result and len(count_result) > 0:
                        row_count = count_result[0].as_py()[0]
                        total_rows += row_count
                        
                        # Estimate bytes per row (very rough estimate)
                        bytes_per_row = 100  # Default estimate
                        schema = self.get_query_schema(source_query)
                        if schema:
                            # Better estimate based on schema
                            bytes_per_row = sum(pa.get_bit_width(field.type) / 8 for field in schema)
                            bytes_per_row = max(bytes_per_row, 10)  # Minimum bytes per row
                        
                        total_bytes += row_count * bytes_per_row
                except Exception as e:
                    print(f"Error estimating query size from {source_name}: {e}")
        
        # If we couldn't estimate, use defaults
        if total_rows == 0:
            total_rows = 1000  # Default estimate
            total_bytes = total_rows * 100  # Default estimate
        
        return total_rows, total_bytes

class FederatedFlightServer(FlightServerBase):
    """
    An Apache Arrow Flight server that supports federated queries across multiple data sources.
    """
    
    def __init__(self, location, config_path, **kwargs):
        super(FederatedFlightServer, self).__init__(location, **kwargs)
        self.data_sources = {}
        self.load_config(config_path)
        self.query_engine = FederatedQueryEngine(self.data_sources)
        print("Federated Flight Server is ready to accept queries!")
    
    def load_config(self, config_path):
        """Load configuration from a JSON file."""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                
            # Initialize data sources
            for source_config in config.get("data_sources", []):
                name = source_config.get("name")
                source_type = source_config.get("type")
                connection_info = source_config.get("connection_info", {})
                
                if name and source_type:
                    # Check for special requirements
                    if source_type == "delta" and not DELTA_AVAILABLE:
                        print(f"Skipping Delta source '{name}': Delta Lake support not available")
                        continue
                    
                    data_source = DataSource(name, source_type, connection_info)
                    self.data_sources[name] = data_source
                    
                    # Test connection
                    if data_source.connect():
                        print(f"Successfully connected to data source: {name}")
                    else:
                        print(f"Failed to connect to data source: {name}")
                        
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error loading config: {e}")
            raise
    
    def _make_flight_info(self, query, descriptor):
        """Create Flight info for a federated query."""
        try:
            # Get schema for the query
            schema = self.query_engine.get_query_schema(query)
            
            # Estimate result size
            count, estimated_size = self.query_engine.estimate_query_size(query)
            
            endpoints = [flight.FlightEndpoint(
                descriptor.command, 
                [flight.Location.for_grpc_tcp("localhost", self.port)]
            )]
            
            return flight.FlightInfo(
                schema=schema,
                descriptor=descriptor,
                endpoints=endpoints,
                total_records=count,
                total_bytes=estimated_size
            )
            
        except Exception as e:
            print(f"Error creating flight info: {e}")
            # Return a minimal FlightInfo
            schema = pa.schema([])
            endpoints = [flight.FlightEndpoint(
                descriptor.command, 
                [flight.Location.for_grpc_tcp("localhost", self.port)]
            )]
            return flight.FlightInfo(
                schema=schema,
                descriptor=descriptor,
                endpoints=endpoints,
                total_records=0,
                total_bytes=0
            )
    
    def get_flight_info(self, context, descriptor):
        """Handle get_flight_info requests."""
        query = descriptor.command.decode('utf-8')
        print(f"Flight info request for query: {query}")
        return self._make_flight_info(query, descriptor)
    
    def do_get(self, context, ticket):
        """Execute a federated query and return results."""
        query = ticket.ticket.decode('utf-8')
        print(f"Executing federated query: {query}")
        
        try:
            # Execute the query and get results as Arrow table
            result = self.query_engine.execute_query(query)
            
            if result is None:
                # Return an empty result
                empty_table = pa.Table.from_arrays([], [])
                return flight.RecordBatchStream(empty_table)
            
            # Return as a stream of Arrow record batches
            return flight.RecordBatchStream(result)
            
        except Exception as e:
            print(f"Error executing query: {e}")
            # Return an empty result
            empty_table = pa.Table.from_arrays([], [])
            return flight.RecordBatchStream(empty_table)
    
    def list_flights(self, context, criteria):
        """List available flights (queries)."""
        # For demonstration, just show a simple example query for each data source
        for source_name in self.data_sources:
            example_query = f"SELECT * FROM {source_name} LIMIT 10"
            descriptor = FlightDescriptor.for_command(example_query.encode('utf-8'))
            yield self._make_flight_info(example_query, descriptor)

def main():
    parser = argparse.ArgumentParser(description='Run a Federated Flight server')
    parser.add_argument('--host', type=str, default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=8815, help='Server port')
    parser.add_argument('--config', type=str, required=True, help='Path to configuration file')
    
    args = parser.parse_args()
    
    location = f"grpc://{args.host}:{args.port}"
    
    server = FederatedFlightServer(
        location=location,
        config_path=args.config
    )
    
    print(f"Starting federated server at {location}")
    server.serve()

if __name__ == '__main__':
    main()