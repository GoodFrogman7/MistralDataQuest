import os
import streamlit as st
import pandas as pd

print("🔍 Starting Streamlit app...")

try:
    from database import Database
    from mistral_service import MistralService
    from data_analysis import analyze_query_results
    from visualization import create_visualization
    print("✅ All modules imported successfully.")
except Exception as e:
    st.error(f"❌ Error during import: {e}")
    print(f"❌ ImportError: {e}")
    raise e

# Set page config
try:
    st.set_page_config(
        page_title="SQLquest - AI-Powered Data Storytelling",
        layout="wide"
    )
    print("✅ Streamlit page config set.")
except Exception as e:
    st.error(f"❌ Error setting page config: {e}")
    print(f"❌ ConfigError: {e}")
    raise e

# Init database
try:
    @st.cache_resource
    def initialize_database(db_path=None, connection_string=None):
        print("📡 Initializing DB...")
        return Database(db_path=db_path, connection_string=connection_string)

    if "db" not in st.session_state:
        st.session_state.db = initialize_database()
        print("✅ Database initialized.")

    db = st.session_state.db
except Exception as e:
    st.error(f"❌ Error initializing DB: {e}")
    print(f"❌ DB Init Error: {e}")
    raise e


# API key management in session state
if "mistral_api_key" not in st.session_state:
    # Initialize with default API key from environment
    st.session_state.mistral_api_key = os.getenv("MISTRAL_API_KEY", "")
    st.session_state.mistral_service = None
    st.session_state.is_using_default_key = True
    
# Create Mistral service if API key is available
def get_mistral_service():
    if not st.session_state.mistral_service and st.session_state.mistral_api_key:
        st.session_state.mistral_service = MistralService(api_key=st.session_state.mistral_api_key)
    return st.session_state.mistral_service

# Initialize Mistral service at startup if default API key is available
if st.session_state.mistral_api_key and not st.session_state.mistral_service:
    get_mistral_service()

# App title and description
st.title("SQLquest - AI-Powered Data Storytelling")
st.markdown("""
Ask questions about your data in plain English and get instant insights with visualizations.
""")

# Sidebar with database connection, info and API key configuration
with st.sidebar:
    st.header("Database Connection")
    
    # Database connection options
    connection_type = st.radio(
        "Database Connection Type",
        ["Use Default Database", "Upload SQLite Database", "Connect to External Database"],
        key="connection_type_radio"
    )
    
    # Handle database connection change
    if connection_type == "Use Default Database" and st.session_state.db_connection_type != "default":
        st.session_state.db_connection_type = "default"
        st.session_state.uploaded_db_path = None
        st.session_state.connection_string = None
        st.session_state.db = initialize_database()
        st.rerun()
    
    # SQLite database upload
    elif connection_type == "Upload SQLite Database":
        uploaded_file = st.file_uploader("Upload SQLite Database File", type=["db", "sqlite", "sqlite3"])
        
        if uploaded_file is not None:
            # Save the uploaded file
            save_dir = "uploaded_db"
            os.makedirs(save_dir, exist_ok=True)
            db_path = os.path.join(save_dir, uploaded_file.name)
            
            # Only process if file is new or different
            process_file = False
            if st.session_state.uploaded_db_path != db_path:
                process_file = True
                
            if process_file:
                with open(db_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                st.success(f"Uploaded SQLite database: {uploaded_file.name}")
                
                # Initialize new database connection
                st.session_state.db_connection_type = "sqlite"
                st.session_state.uploaded_db_path = db_path
                st.session_state.db = initialize_database(db_path=db_path)
                st.rerun()
    
    # External database connection
    elif connection_type == "Connect to External Database":
        st.markdown("""Enter your database connection string:""")
        db_type = st.selectbox("Database Type", ["PostgreSQL", "MySQL", "SQLite"])
        
        if db_type == "PostgreSQL":
            host = st.text_input("Host", "localhost")
            port = st.text_input("Port", "5432")
            database = st.text_input("Database Name")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            if st.button("Connect"):
                if database and username and password:
                    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
                    try:
                        st.session_state.db_connection_type = "external"
                        st.session_state.connection_string = connection_string
                        st.session_state.db = initialize_database(connection_string=connection_string)
                        st.success("Connected to PostgreSQL database successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error connecting to database: {str(e)}")
                else:
                    st.warning("Please fill in all required fields")
        
        elif db_type == "MySQL":
            host = st.text_input("Host", "localhost")
            port = st.text_input("Port", "3306")
            database = st.text_input("Database Name")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            if st.button("Connect"):
                if database and username:
                    connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
                    try:
                        st.session_state.db_connection_type = "external"
                        st.session_state.connection_string = connection_string
                        st.session_state.db = initialize_database(connection_string=connection_string)
                        st.success("Connected to MySQL database successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error connecting to database: {str(e)}")
                else:
                    st.warning("Please fill in all required fields")
        
        elif db_type == "SQLite":
            path = st.text_input("Database File Path (absolute path)")
            
            if st.button("Connect"):
                if path:
                    try:
                        st.session_state.db_connection_type = "external_sqlite"
                        st.session_state.uploaded_db_path = path
                        st.session_state.db = initialize_database(db_path=path)
                        st.success("Connected to SQLite database successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error connecting to database: {str(e)}")
                else:
                    st.warning("Please provide the database file path")
    
    # Database information section
    st.header("Database Information")
    
    # Display the current connection type
    if st.session_state.db_connection_type == "default":
        st.info("Using default database")
    elif st.session_state.db_connection_type == "sqlite":
        db_name = os.path.basename(st.session_state.uploaded_db_path) if st.session_state.uploaded_db_path else "Unknown"
        st.info(f"Using uploaded SQLite database: {db_name}")
    elif st.session_state.db_connection_type == "external":
        st.info("Using external database connection")
    elif st.session_state.db_connection_type == "external_sqlite":
        db_name = os.path.basename(st.session_state.uploaded_db_path) if st.session_state.uploaded_db_path else "Unknown"
        st.info(f"Using external SQLite database: {db_name}")
    
    # Display database schema information
    st.subheader("Available Tables")
    schema_info = db.get_schema_info()
    
    if schema_info:
        for table_name, columns in schema_info.items():
            with st.expander(f"{table_name}"):
                for col in columns:
                    st.text(f"• {col['name']} ({col['type']})")
    else:
        st.warning("No tables found in the connected database.")
    
    st.divider()
    
    # Mistral API Configuration
    st.subheader("Mistral API Configuration")
    
    # Display current API key status
    if st.session_state.mistral_api_key:
        is_default_key = st.session_state.mistral_api_key == os.getenv("MISTRAL_API_KEY", "")
        if is_default_key:
            st.success("Using default Mistral API key.")
        else:
            st.success("Using custom Mistral API key.")
    else:
        st.warning("Mistral API key is not set")
    
    # Track if we're using a custom key
    if "using_custom_key" not in st.session_state:
        st.session_state.using_custom_key = False
        
    # Checkbox to use custom API key
    use_custom_key = st.checkbox("Use my own Mistral API key", value=st.session_state.using_custom_key, 
                           help="Uncheck to use the default Mistral API key provided by the application")
    
    # Handle state change in checkbox
    if use_custom_key != st.session_state.using_custom_key:
        st.session_state.using_custom_key = use_custom_key
        
        # If switching back to default key
        if not use_custom_key:
            default_api_key = os.getenv("MISTRAL_API_KEY", "")
            st.session_state.mistral_api_key = default_api_key
            st.session_state.mistral_service = None
            st.success("Switched to default Mistral API key")
            get_mistral_service()  # Initialize with default key
            st.rerun()
    
    # Input field for API key (only shown if checkbox is checked)
    if use_custom_key:
        # Show text input for custom key
        custom_key_input = st.text_input(
            "Enter your Mistral API Key", 
            value="",
            type="password",
            help="Enter your Mistral API key to enable natural language processing"
        )
        
        # Update API key if provided
        if custom_key_input and custom_key_input != st.session_state.mistral_api_key:
            st.session_state.mistral_api_key = custom_key_input
            st.session_state.mistral_service = None
            st.success("Custom API key applied!")
            get_mistral_service()  # Initialize the service with the new key
    else:
        # Make sure we're using the default key from environment
        default_api_key = os.getenv("MISTRAL_API_KEY", "")
        if default_api_key and not st.session_state.mistral_api_key:
            st.session_state.mistral_api_key = default_api_key
            st.session_state.mistral_service = None
            get_mistral_service()  # Initialize the service with the default key
    
    st.divider()
    
    # About section
    st.subheader("About SQLquest")
    st.markdown("""
    SQLquest converts your natural language questions into SQL queries,
    provides narrative insights, and creates visualizations to help you
    understand your data better.
    """)

# Main input area
query_input = st.text_area("Ask a question about your data", height=100, 
                         placeholder="e.g., 'Show me the top 5 products by sales' or 'What departments have the highest average salary?'")

# Tone selection
tone = st.selectbox("Choose narrative tone", ["Formal", "Casual"])

# Submit button
submit_button = st.button("Get Insights", type="primary")

# Process the query when submit button is clicked
if submit_button and query_input:
    # Check if Mistral API key is available
    mistral_service = get_mistral_service()
    if not mistral_service:
        st.error("No valid Mistral API key found. The application is using the default API key, but it appears to be invalid or missing. Please check with the administrator.")
    else:
        with st.spinner("Processing your question..."):
            try:
                # Generate SQL query from natural language
                # Pass the database type to the Mistral service for better SQL generation
                sql_query, error = mistral_service.generate_sql(
                    query_input, 
                    db.get_schema_info(),
                    db_type=db.db_type
                )
                
                if error:
                    st.error(f"Error generating SQL query: {error}")
                else:
                    # Display the generated SQL in a collapsible expander
                    with st.expander("View Generated SQL Query", expanded=False):
                        st.code(sql_query, language="sql")
                    
                    # Execute the query
                    try:
                        results_df = db.execute_query(sql_query)
                        
                        if results_df is not None and not results_df.empty:
                            # Analyze the results
                            analysis = analyze_query_results(results_df)
                            
                            # Generate narrative insights
                            narrative = mistral_service.generate_narrative(
                                query_input, 
                                sql_query, 
                                results_df, 
                                analysis, 
                                tone.lower()
                            )
                            
                            # Display results in two columns
                            col1, col2 = st.columns([3, 2])
                            
                            with col1:
                                # Display narrative insights
                                st.subheader("Insights")
                                st.markdown(narrative)
                                
                                # Display data table
                                st.subheader("Data")
                                st.dataframe(results_df)
                            
                            with col2:
                                # Create and display visualization
                                st.subheader("Visualization")
                                viz_fig = create_visualization(results_df, query_input)
                                st.plotly_chart(viz_fig, use_container_width=True)
                        else:
                            st.info("The query returned no results.")
                    except Exception as e:
                        st.error(f"Error executing query: {str(e)}")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
elif submit_button:
    st.warning("Please enter a question to proceed.")

# Display sample questions to help users get started
if not submit_button:
    st.subheader("Sample Questions")
    st.markdown("""
    Try asking questions like:
    - What are the top 10 most expensive products?
    - Show me the average sales by region
    - Which employees have been with the company the longest?
    - Compare revenue across different departments
    """)

# Add footer
st.markdown("---")
st.markdown("""
<div style="text-align: center">
SQLquest - AI-Powered Data Storytelling Platform
</div>
""", unsafe_allow_html=True)
