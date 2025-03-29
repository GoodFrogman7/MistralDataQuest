import os
import streamlit as st
import pandas as pd

from database import Database
from mistral_service import MistralService
from data_analysis import analyze_query_results
from visualization import create_visualization

# Set page configuration
st.set_page_config(
    page_title="SQLquest - AI-Powered Data Storytelling",
    layout="wide"
)

# Initialize database connection and Mistral service
@st.cache_resource
def initialize_services():
    db = Database()
    mistral = MistralService(api_key=os.getenv("MISTRAL_API_KEY", ""))
    return db, mistral

db, mistral = initialize_services()

# App title and description
st.title("SQLquest - AI-Powered Data Storytelling")
st.markdown("""
Ask questions about your data in plain English and get instant insights with visualizations.
""")

# Sidebar with database info
with st.sidebar:
    st.header("Database Information")
    
    # Display database schema information
    st.subheader("Available Tables")
    schema_info = db.get_schema_info()
    
    for table_name, columns in schema_info.items():
        with st.expander(f"{table_name}"):
            for col in columns:
                st.text(f"• {col['name']} ({col['type']})")
    
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
    with st.spinner("Processing your question..."):
        try:
            # Generate SQL query from natural language
            sql_query, error = mistral.generate_sql(query_input, db.get_schema_info())
            
            if error:
                st.error(f"Error generating SQL query: {error}")
            else:
                # Display the generated SQL
                with st.expander("Generated SQL Query", expanded=True):
                    st.code(sql_query, language="sql")
                
                # Execute the query
                try:
                    results_df = db.execute_query(sql_query)
                    
                    if results_df is not None and not results_df.empty:
                        # Analyze the results
                        analysis = analyze_query_results(results_df)
                        
                        # Generate narrative insights
                        narrative = mistral.generate_narrative(
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
