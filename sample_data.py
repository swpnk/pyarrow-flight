# generate_sample_data.py
# Generates a sample Parquet file with 100k records

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_sample_data(num_records=100000, output_path="sample_data.parquet"):
    """
    Generate a sample Parquet file with random data
    
    Parameters:
    -----------
    num_records: int
        Number of records to generate
    output_path: str
        Path to save the Parquet file
    """
    print(f"Generating {num_records} records...")
    
    # Generate random data
    np.random.seed(42)  # For reproducibility
    
    # Generate a range of dates
    start_date = datetime(2020, 1, 1)
    dates = [start_date + timedelta(days=i % 365) for i in range(num_records)]
    
    # Generate categorical data
    categories = ['A', 'B', 'C', 'D', 'E']
    category_data = np.random.choice(categories, size=num_records)
    
    # Generate numeric data
    numeric_data1 = np.random.normal(100, 15, size=num_records)
    numeric_data2 = np.random.exponential(50, size=num_records)
    
    # Generate boolean data
    boolean_data = np.random.choice([True, False], size=num_records)
    
    # Generate string data
    string_prefixes = ['foo', 'bar', 'baz', 'qux', 'quux']
    string_data = [f"{np.random.choice(string_prefixes)}_{i % 1000}" for i in range(num_records)]
    
    # Create a DataFrame
    df = pd.DataFrame({
        'id': range(1, num_records + 1),
        'date': dates,
        'category': category_data,
        'value1': numeric_data1,
        'value2': numeric_data2,
        'active': boolean_data,
        'name': string_data
    })
    
    # Save as Parquet
    df.to_parquet(output_path, index=False)
    
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Generated {num_records} records, saved to {output_path} ({file_size_mb:.2f} MB)")
    
    # Show sample of the data
    print("\nSample data (first 5 rows):")
    print(df.head())

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate sample Parquet data')
    parser.add_argument('--num-records', type=int, default=100000, 
                        help='Number of records to generate')
    parser.add_argument('--output-path', type=str, default="sample_data.parquet",
                        help='Path to save the Parquet file')
    
    args = parser.parse_args()
    
    generate_sample_data(
        num_records=args.num_records,
        output_path=args.output_path
    )