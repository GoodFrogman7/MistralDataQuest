import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

class Database:
    def __init__(self, db_path=None):
        """
        Initialize database connection using SQLAlchemy.
        Supports SQLite by default, with possible extension to other databases.
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.engine = None
        self.inspector = None
        
        # If no db_path is provided, create/use a default SQLite database
        if not db_path:
            # Create sample database if it doesn't exist
            self._create_sample_db()
            db_path = "data/sample.db"
        
        # Connect to the database
        try:
            self.engine = create_engine(f"sqlite:///{db_path}")
            self.inspector = inspect(self.engine)
            print(f"Successfully connected to database")
        except SQLAlchemyError as e:
            print(f"Error connecting to database: {str(e)}")
    
    def _create_sample_db(self):
        """Create a sample database with tables if it doesn't exist"""
        os.makedirs("data", exist_ok=True)
        
        conn = sqlite3.connect("data/sample.db")
        cursor = conn.cursor()
        
        # Create employees table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS employees (
            employee_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department TEXT NOT NULL,
            salary REAL NOT NULL,
            hire_date TEXT NOT NULL
        )
        ''')
        
        # Create products table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL
        )
        ''')
        
        # Create sales table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales (
            sale_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            employee_id INTEGER,
            sale_date TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            total_amount REAL NOT NULL,
            region TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (product_id),
            FOREIGN KEY (employee_id) REFERENCES employees (employee_id)
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_schema_info(self):
        """
        Get database schema information for all tables
        
        Returns:
            dict: Dictionary with table names as keys and column information as values
        """
        if not self.inspector:
            return {}
        
        schema_info = {}
        
        for table_name in self.inspector.get_table_names():
            columns = []
            for column in self.inspector.get_columns(table_name):
                columns.append({
                    'name': column['name'],
                    'type': str(column['type']).lower()
                })
            
            foreign_keys = self.inspector.get_foreign_keys(table_name)
            primary_key = self.inspector.get_pk_constraint(table_name)
            
            for col in columns:
                # Mark primary key columns
                if primary_key and col['name'] in primary_key['constrained_columns']:
                    col['is_primary_key'] = True
                
                # Mark foreign key columns
                for fk in foreign_keys:
                    if col['name'] in fk['constrained_columns']:
                        col['references'] = {
                            'table': fk['referred_table'],
                            'column': fk['referred_columns'][0]
                        }
            
            schema_info[table_name] = columns
        
        return schema_info
    
    def execute_query(self, query):
        """
        Execute SQL query safely using SQLAlchemy parameters
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            pandas.DataFrame: Query results as a DataFrame
        """
        try:
            # Execute the query
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                # Convert result to DataFrame
                df = pd.DataFrame(result.fetchall())
                if not df.empty:
                    df.columns = result.keys()
                return df
        except SQLAlchemyError as e:
            print(f"Error executing query: {str(e)}")
            raise e
