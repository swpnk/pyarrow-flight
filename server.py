# server.py
# Flight server that uses DuckDB to query Parquet files

import os
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq
import duckdb
import argparse
from pyarrow.flight import FlightServerBase, ServerAuthHandler, FlightDescriptor

class ParquetQueryService(FlightServerBase):
    def __init__(self, location, parquet_path, **kwargs):
        super(ParquetQueryService, self).__init__(location, **kwargs)
        self.parquet_path = parquet_path
        # Initialize DuckDB connection
        self.conn = duckdb.connect(database=':memory:', read_only=False)
        print(f"Loading Parquet file: {parquet_path}")
        # Register the Parquet file as a view in DuckDB
        self.conn.execute(f"CREATE VIEW dataset AS SELECT * FROM parquet_scan('{parquet_path}')")
        print("Server is ready to accept queries!")

    def _make_flight_info(self, query, descriptor):
        # Get schema by executing the query with LIMIT 0
        schema_query = f"SELECT * FROM ({query}) AS subquery LIMIT 0"
        result = self.conn.execute(schema_query).arrow()
        
        # Estimate dataset size - this is an approximation
        size_query = f"SELECT COUNT(*) FROM ({query}) AS subquery"
        count = self.conn.execute(size_query).fetchone()[0]
        estimated_size = count * 100  # Rough estimate: 100 bytes per row
        
        endpoints = [flight.FlightEndpoint(
            descriptor.command, 
            [flight.Location.for_grpc_tcp("localhost", self.port)]
        )]
        
        return flight.FlightInfo(
            schema=result.schema,
            descriptor=descriptor,
            endpoints=endpoints,
            total_records=count,
            total_bytes=estimated_size
        )

    def get_flight_info(self, context, descriptor):
        query = descriptor.command.decode('utf-8')
        print(f"Flight info request for query: {query}")
        return self._make_flight_info(query, descriptor)

    def do_get(self, context, ticket):
        query = ticket.ticket.decode('utf-8')
        print(f"Executing query: {query}")
        
        # Execute the query and get results as Arrow table
        result = self.conn.execute(query).arrow()
        
        # Return as a stream of Arrow record batches
        return flight.RecordBatchStream(result)

    def list_flights(self, context, criteria):
        example_query = "SELECT * FROM dataset LIMIT 10"
        descriptor = FlightDescriptor.for_command(example_query.encode('utf-8'))
        yield self._make_flight_info(example_query, descriptor)

def main():
    parser = argparse.ArgumentParser(description='Run a Flight server with DuckDB for Parquet files')
    parser.add_argument('--host', type=str, default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=8815, help='Server port')
    parser.add_argument('--parquet-path', type=str, required=True, help='Path to Parquet file')
    
    args = parser.parse_args()
    
    location = f"grpc://{args.host}:{args.port}"
    
    server = ParquetQueryService(
        location=location,
        parquet_path=args.parquet_path
    )
    
    print(f"Starting server at {location}")
    server.serve()

if __name__ == '__main__':
    main()

