# client.py
# Flight client that connects to the server, executes queries, and retrieves results

import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import argparse
import time
import os

def run_query(host, port, query, output_format="csv", output_path=None):
    """
    Run a query against the Flight server and retrieve results
    
    Parameters:
    -----------
    host: str
        The hostname of the Flight server
    port: int
        The port of the Flight server
    query: str
        The SQL query to execute
    output_format: str
        The format to output the results in (csv, parquet, or display)
    output_path: str
        The path to save the results to (if None, will use default name)
    """
    # Create a Flight client
    client = flight.FlightClient(f"grpc://{host}:{port}")
    
    # Create a Flight descriptor for the query
    descriptor = flight.FlightDescriptor.for_command(query.encode('utf-8'))
    
    print(f"Executing query: {query}")
    start_time = time.time()
    
    # Get Flight info to understand the schema and endpoints
    flight_info = client.get_flight_info(descriptor)
    
    # Get the first (and only) endpoint
    endpoint = flight_info.endpoints[0]
    
    # Connect to the endpoint and get the data
    ticket = endpoint.ticket
    reader = client.do_get(ticket)
    
    # Read all batches
    table = reader.read_all()
    
    end_time = time.time()
    print(f"Query executed in {end_time - start_time:.2f} seconds")
    print(f"Retrieved {len(table)} rows")
    
    # Process the results based on the output format
    if output_format == "display":
        # Convert to pandas and display the first 10 rows
        df = table.to_pandas()
        print("\nFirst 10 rows:")
        print(df.head(10))
        
    elif output_format == "csv":
        # Convert to pandas and save as CSV
        if output_path is None:
            output_path = "query_results.csv"
        
        df = table.to_pandas()
        df.to_csv(output_path, index=False)
        print(f"Results saved to {output_path}")
        
    elif output_format == "parquet":
        # Save as Parquet
        if output_path is None:
            output_path = "query_results.parquet"
        
        pq.write_table(table, output_path)
        print(f"Results saved to {output_path}")
    
    return table

def main():
    parser = argparse.ArgumentParser(description='Run a query against a Flight server with DuckDB')
    parser.add_argument('--host', type=str, default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=8815, help='Server port')
    parser.add_argument('--query', type=str, required=True, help='SQL query to execute')
    parser.add_argument('--output-format', type=str, choices=['csv', 'parquet', 'display'], 
                        default='csv', help='Output format')
    parser.add_argument('--output-path', type=str, default=None, 
                        help='Path to save the results to')
    
    args = parser.parse_args()
    
    run_query(
        host=args.host,
        port=args.port,
        query=args.query,
        output_format=args.output_format,
        output_path=args.output_path
    )

if __name__ == '__main__':
    main()