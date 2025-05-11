# DuckDB + Arrow Flight + Parquet Query Service

A high-performance data query service that allows users to execute SQL queries against Parquet files and download results in various formats.

## Architecture Overview

```
┌─────────────┐         ┌─────────────────┐         ┌───────────────┐
│             │ SQL     │                 │ SQL     │               │
│   Client    │────────>│  Flight Server  │────────>│    DuckDB     │
│             │         │                 │         │               │
└─────────────┘         └─────────────────┘         └───────┬───────┘
       ▲                         │                          │
       │                         │                          │
       │         Arrow           │                          │
       │         Data            │                          ▼
       └─────────────────────────┘                  ┌───────────────┐
                                                    │               │
                                                    │  Parquet File │
                                                    │               │
                                                    └───────────────┘
```

## Overview

This project provides a complete solution for efficiently querying large Parquet datasets using DuckDB and Apache Arrow Flight. The system consists of:

1. **Arrow Flight Server** - A service that connects to Parquet files using DuckDB and exposes a Flight API
2. **Arrow Flight Client** - A client application for sending SQL queries and retrieving results
3. **Data Generator** - A utility for creating sample Parquet data for testing

## Features

- 🚀 **High-performance data transfer** with Apache Arrow Flight protocol
- 📊 **Efficient SQL execution** against Parquet files using DuckDB
- 📦 **Multiple output formats** including CSV and Parquet
- 🔄 **Streaming data retrieval** for handling large result sets
- 📈 **Schema inference** and metadata handling

## Installation

### Prerequisites

- Python 3.8+
- pip package manager

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/duckdb-flight-parquet.git
   cd duckdb-flight-parquet
   ```

2. Install dependencies:
   ```bash
   pip install pyarrow duckdb pandas
   ```

## Components

### 1. Data Generator (`generate_sample_data.py`)

Generates a sample Parquet file with 100,000 records containing:
- Sequential IDs
- Dates
- Categorical variables
- Numeric values (normal and exponential distributions)
- Boolean flags
- String data

### 2. Flight Server (`server.py`)

A server that:
- Connects to Parquet files using DuckDB
- Exposes a Flight API for executing SQL queries
- Returns results in Arrow format

### 3. Flight Client (`client.py`)

A client that:
- Connects to the Flight server
- Sends SQL queries
- Retrieves and processes results

## Usage

### Step 1: Generate Sample Data

```bash
python generate_sample_data.py --num-records 100000 --output-path sample_data.parquet
```

Options:
- `--num-records`: Number of records to generate (default: 100000)
- `--output-path`: Path to save the Parquet file (default: sample_data.parquet)

### Step 2: Start the Flight Server

```bash
python server.py --host localhost --port 8815 --parquet-path sample_data.parquet
```

Options:
- `--host`: Server host (default: localhost)
- `--port`: Server port (default: 8815)
- `--parquet-path`: Path to the Parquet file to serve (required)

### Step 3: Execute Queries with the Client

```bash
python client.py --query "SELECT * FROM dataset LIMIT 100" --output-format csv --output-path results.csv
```

Options:
- `--host`: Server host (default: localhost)
- `--port`: Server port (default: 8815)
- `--query`: SQL query to execute (required)
- `--output-format`: Format to output the results (csv, parquet, or display)
- `--output-path`: Path to save the results (default: query_results.csv or query_results.parquet)

## Example Queries

```sql
-- Basic filtering
SELECT * FROM dataset WHERE category = 'A' LIMIT 1000

-- Aggregation
SELECT category, 
       COUNT(*) as count, 
       AVG(value1) as avg_value1, 
       SUM(value2) as sum_value2 
FROM dataset 
GROUP BY category

-- Date filtering
SELECT * FROM dataset 
WHERE date BETWEEN '2020-01-01' AND '2020-01-31'

-- Complex analysis
SELECT 
  category,
  DATE_TRUNC('month', date) as month,
  COUNT(*) as count,
  AVG(value1) as avg_value1,
  SUM(CASE WHEN active THEN 1 ELSE 0 END) as active_count
FROM dataset
GROUP BY category, DATE_TRUNC('month', date)
ORDER BY month, category
```

## How It Works

### Data Flow

1. **Client initiates query**:
   - Client sends SQL query to server
   - Server determines result schema and size
   - Client receives metadata (schema, endpoints)

2. **Data retrieval**:
   - Client requests data using endpoint info
   - Server executes query with DuckDB
   - Results streamed back as Arrow record batches
   - Client processes and outputs data

### Technical Implementation

#### Server-side Processing

The server uses DuckDB to efficiently query Parquet files:

```python
# Register Parquet file as a view
self.conn.execute(f"CREATE VIEW dataset AS SELECT * FROM parquet_scan('{parquet_path}')")

# Execute query and convert to Arrow
result = self.conn.execute(query).arrow()
```

#### Data Transfer

Apache Arrow Flight handles efficient data transfer:

```python
# Server: Return result as a stream of record batches
return flight.RecordBatchStream(result)

# Client: Receive all batches
table = reader.read_all()
```

## Performance Considerations

This solution is optimized for performance in several ways:

1. **DuckDB Optimizations**:
   - Parallel query execution
   - Vectorized processing
   - Predicate pushdown to Parquet files
   - Statistics-based optimizations

2. **Arrow Flight Benefits**:
   - Minimal serialization/deserialization overhead
   - Columnar data format reduces memory usage
   - Efficient network protocol with streaming support
   - Schema-aware data transfer

3. **Memory Efficiency**:
   - Data remains in columnar format
   - Streaming approach for large result sets

## Advanced Usage

### Working with Multiple Parquet Files

Modify the server to handle multiple Parquet files:

```python
# Register multiple Parquet files in a directory
def register_parquet_files(self, directory):
    files = glob.glob(f"{directory}/*.parquet")
    views = []
    
    for i, file in enumerate(files):
        view_name = f"file_{i}"
        self.conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM parquet_scan('{file}')")
        views.append(view_name)
    
    # Create a union view of all files
    union_query = " UNION ALL ".join([f"SELECT * FROM {view}" for view in views])
    self.conn.execute(f"CREATE VIEW dataset AS {union_query}")
```

### Adding Authentication

Implement basic authentication:

```python
class AuthHandler(flight.ServerAuthHandler):
    def authenticate(self, outgoing, incoming):
        auth_bytes = incoming.read()
        auth = json.loads(auth_bytes.decode('utf-8'))
        
        if auth['username'] != 'user' or auth['password'] != 'password':
            raise flight.FlightUnauthenticatedError("Invalid credentials")
        
        outgoing.write(b"token")
    
    def is_valid(self, token):
        return token == b"token"

# Use in server initialization
server = ParquetQueryService(
    location=location,
    parquet_path=args.parquet_path, 
    auth_handler=AuthHandler()
)
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **Connection refused** | Check if server is running and port is correct |
| **Invalid SQL syntax** | Ensure query is compatible with DuckDB SQL dialect |
| **Memory errors** | Add LIMIT clause or filter data more aggressively |
| **Slow performance** | Add indexes or use more selective filters |

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Apache Arrow](https://arrow.apache.org/) for the Arrow Flight protocol
- [DuckDB](https://duckdb.org/) for the efficient SQL engine
- [PyArrow](https://arrow.apache.org/docs/python/) for Python bindings
