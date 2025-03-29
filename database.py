import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

class Database:
    def __init__(self, db_path=None, connection_string=None):
        """
        Initialize database connection using SQLAlchemy.
        Supports SQLite by default, with possible extension to other databases.
        
        Args:
            db_path (str): Path to the SQLite database file
            connection_string (str): SQLAlchemy connection string for external databases
        """
        self.engine = None
        self.inspector = None
        self.db_type = "sqlite"  # Default database type
        
        # If external connection string is provided, use it
        if connection_string:
            try:
                self.engine = create_engine(connection_string)
                self.inspector = inspect(self.engine)
                if "postgresql" in connection_string:
                    self.db_type = "postgresql"
                elif "mysql" in connection_string:
                    self.db_type = "mysql"
                print(f"Successfully connected to external database")
            except SQLAlchemyError as e:
                print(f"Error connecting to external database: {str(e)}")
                # Fallback to environment PostgreSQL or SQLite
                self._connect_to_default_db(db_path)
        else:
            # Connect to default database (PostgreSQL from env or SQLite)
            self._connect_to_default_db(db_path)
    
    def _connect_to_default_db(self, db_path=None):
        """Connect to default database - PostgreSQL from env vars or SQLite"""
        # Check for PostgreSQL connection from environment variables
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            try:
                self.engine = create_engine(database_url)
                self.inspector = inspect(self.engine)
                self.db_type = "postgresql"
                # Check if the database has tables, if not, create sample tables
                if not self.inspector.get_table_names():
                    print("PostgreSQL database is empty. Creating sample tables...")
                    self._create_sample_tables()
                print(f"Successfully connected to PostgreSQL database")
            except SQLAlchemyError as e:
                print(f"Error connecting to PostgreSQL database: {str(e)}")
                # Fallback to SQLite
                self._setup_sqlite(db_path)
        else:
            # Use SQLite if no PostgreSQL connection is available
            self._setup_sqlite(db_path)
    
    def _setup_sqlite(self, db_path=None):
        """Set up SQLite database connection"""
        if not db_path:
            os.makedirs("data", exist_ok=True)
            db_path = "data/sample.db"
            
            # Remove existing database file if it's corrupted
            if os.path.exists(db_path):
                try:
                    # Test if the file is a valid database
                    test_conn = sqlite3.connect(db_path)
                    test_conn.execute("SELECT 1")
                    test_conn.close()
                except sqlite3.DatabaseError:
                    print("Removing corrupted database file")
                    os.remove(db_path)
            
            # Create a new database file
            self._create_sample_tables_sqlite(db_path)
        
        try:
            self.engine = create_engine(f"sqlite:///{db_path}")
            self.inspector = inspect(self.engine)
            print(f"Successfully connected to SQLite database")
        except SQLAlchemyError as e:
            print(f"Error connecting to SQLite database: {str(e)}")
    
    def _create_sample_tables_sqlite(self, db_path):
        """Create sample tables in SQLite database"""
        conn = sqlite3.connect(db_path)
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
        
        # Insert sample data for employees
        cursor.execute("INSERT INTO employees (name, department, salary, hire_date) VALUES (?, ?, ?, ?)",
                      ("John Smith", "Engineering", 85000.00, "2021-05-15"))
        cursor.execute("INSERT INTO employees (name, department, salary, hire_date) VALUES (?, ?, ?, ?)",
                      ("Sarah Johnson", "Marketing", 72000.00, "2022-01-10"))
        cursor.execute("INSERT INTO employees (name, department, salary, hire_date) VALUES (?, ?, ?, ?)",
                      ("Michael Chen", "Engineering", 95000.00, "2020-08-22"))
        cursor.execute("INSERT INTO employees (name, department, salary, hire_date) VALUES (?, ?, ?, ?)",
                      ("Emily Davis", "Sales", 68000.00, "2022-04-05"))
        cursor.execute("INSERT INTO employees (name, department, salary, hire_date) VALUES (?, ?, ?, ?)",
                      ("Robert Wilson", "Finance", 78000.00, "2021-10-18"))
        
        # Insert sample data for products
        cursor.execute("INSERT INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)",
                      ("Laptop Pro", "Electronics", 1299.99, 45))
        cursor.execute("INSERT INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)",
                      ("Deluxe Headphones", "Electronics", 249.99, 78))
        cursor.execute("INSERT INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)",
                      ("Smart Watch", "Electronics", 399.99, 32))
        cursor.execute("INSERT INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)",
                      ("Office Chair", "Furniture", 199.99, 15))
        cursor.execute("INSERT INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)",
                      ("Ergonomic Desk", "Furniture", 349.99, 12))
        cursor.execute("INSERT INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)",
                      ("Wireless Mouse", "Electronics", 59.99, 90))
        cursor.execute("INSERT INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)",
                      ("External SSD", "Electronics", 149.99, 28))
        
        # Insert sample data for sales
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (1, 4, "2023-10-05", 2, 2599.98, "West"))
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (3, 4, "2023-10-07", 3, 1199.97, "East"))
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (2, 4, "2023-11-12", 5, 1249.95, "West"))
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (5, 4, "2023-11-15", 1, 349.99, "North"))
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (7, 2, "2023-12-01", 4, 599.96, "South"))
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (6, 2, "2023-12-05", 10, 599.90, "East"))
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (4, 4, "2024-01-10", 2, 399.98, "West"))
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (1, 4, "2024-01-15", 1, 1299.99, "South"))
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (3, 2, "2024-02-02", 2, 799.98, "North"))
        cursor.execute("INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES (?, ?, ?, ?, ?, ?)",
                      (2, 4, "2024-02-15", 1, 249.99, "East"))
        
        conn.commit()
        conn.close()
        print("Sample SQLite database created successfully")
    
    def _create_sample_tables(self):
        """Create sample tables in PostgreSQL database"""
        with self.engine.connect() as connection:
            # Create employees table
            connection.execute(text('''
            CREATE TABLE IF NOT EXISTS employees (
                employee_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                salary DECIMAL(10, 2) NOT NULL,
                hire_date DATE NOT NULL
            )
            '''))
            
            # Create products table
            connection.execute(text('''
            CREATE TABLE IF NOT EXISTS products (
                product_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                stock INTEGER NOT NULL
            )
            '''))
            
            # Create sales table
            connection.execute(text('''
            CREATE TABLE IF NOT EXISTS sales (
                sale_id SERIAL PRIMARY KEY,
                product_id INTEGER REFERENCES products(product_id),
                employee_id INTEGER REFERENCES employees(employee_id),
                sale_date DATE NOT NULL,
                quantity INTEGER NOT NULL,
                total_amount DECIMAL(10, 2) NOT NULL,
                region TEXT NOT NULL
            )
            '''))
            
            # Insert sample data for employees
            connection.execute(text('''
            INSERT INTO employees (name, department, salary, hire_date) VALUES 
            ('John Smith', 'Engineering', 85000.00, '2021-05-15'),
            ('Sarah Johnson', 'Marketing', 72000.00, '2022-01-10'),
            ('Michael Chen', 'Engineering', 95000.00, '2020-08-22'),
            ('Emily Davis', 'Sales', 68000.00, '2022-04-05'),
            ('Robert Wilson', 'Finance', 78000.00, '2021-10-18')
            '''))
            
            # Insert sample data for products
            connection.execute(text('''
            INSERT INTO products (name, category, price, stock) VALUES 
            ('Laptop Pro', 'Electronics', 1299.99, 45),
            ('Deluxe Headphones', 'Electronics', 249.99, 78),
            ('Smart Watch', 'Electronics', 399.99, 32),
            ('Office Chair', 'Furniture', 199.99, 15),
            ('Ergonomic Desk', 'Furniture', 349.99, 12),
            ('Wireless Mouse', 'Electronics', 59.99, 90),
            ('External SSD', 'Electronics', 149.99, 28)
            '''))
            
            # Insert sample data for sales
            connection.execute(text('''
            INSERT INTO sales (product_id, employee_id, sale_date, quantity, total_amount, region) VALUES 
            (1, 4, '2023-10-05', 2, 2599.98, 'West'),
            (3, 4, '2023-10-07', 3, 1199.97, 'East'),
            (2, 4, '2023-11-12', 5, 1249.95, 'West'),
            (5, 4, '2023-11-15', 1, 349.99, 'North'),
            (7, 2, '2023-12-01', 4, 599.96, 'South'),
            (6, 2, '2023-12-05', 10, 599.90, 'East'),
            (4, 4, '2024-01-10', 2, 399.98, 'West'),
            (1, 4, '2024-01-15', 1, 1299.99, 'South'),
            (3, 2, '2024-02-02', 2, 799.98, 'North'),
            (2, 4, '2024-02-15', 1, 249.99, 'East')
            '''))
            
            connection.commit()
            print("Sample PostgreSQL database created successfully")
    
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
