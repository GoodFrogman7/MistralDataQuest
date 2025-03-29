import os
import json
import requests
from typing import Tuple, Dict, Any, Optional, List
import pandas as pd

class MistralService:
    def __init__(self, api_key: str):
        """
        Initialize the Mistral API service.
        
        Args:
            api_key (str): Mistral API key
        """
        self.api_key = api_key
        self.api_url = "https://api.mistral.ai/v1/chat/completions"
        self.model = "mistral-large-latest"  # Use the most advanced model available
    
    def _call_mistral_api(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Make a call to the Mistral API.
        
        Args:
            messages (List[Dict[str, str]]): List of message objects for the conversation
            
        Returns:
            Dict[str, Any]: API response
        """
        if not self.api_key:
            raise ValueError("Mistral API key is not set. Please set the MISTRAL_API_KEY environment variable.")
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        data = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.1,  # Low temperature for more deterministic responses
            "max_tokens": 2048
        }
        
        try:
            response = requests.post(self.api_url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error calling Mistral API: {str(e)}")
            raise
    
    def generate_sql(self, natural_language_query: str, schema_info: Dict) -> Tuple[str, Optional[str]]:
        """
        Generate SQL query from natural language using Mistral.
        
        Args:
            natural_language_query (str): User's natural language question
            schema_info (Dict): Database schema information
            
        Returns:
            Tuple[str, Optional[str]]: Generated SQL query and error message if any
        """
        # Format schema info for the prompt
        schema_text = "Database Schema:\n"
        for table_name, columns in schema_info.items():
            schema_text += f"Table: {table_name}\n"
            for col in columns:
                schema_text += f"  - {col['name']} ({col['type']})"
                if 'is_primary_key' in col and col['is_primary_key']:
                    schema_text += " (PRIMARY KEY)"
                if 'references' in col:
                    schema_text += f" (FOREIGN KEY to {col['references']['table']}.{col['references']['column']})"
                schema_text += "\n"
            schema_text += "\n"
        
        # Determine if we're using PostgreSQL or SQLite based on schema info
        is_postgresql = any("serial" in str(col.get('type', '')).lower() for table in schema_info.values() for col in table)
        
        # Construct the prompt with clearer instructions
        prompt = f"""You are an expert SQL query generator. Your task is to convert a natural language question into a valid SQL query.

{schema_text}

Question: {natural_language_query}

IMPORTANT: Respond with ONLY the SQL query. No explanation, no code blocks, no backticks, no markdown formatting.
Your response should begin with SELECT or WITH and be a valid, executable SQL query.

Generate a valid SQL query that answers this question. The query should:
1. Be compatible with {"PostgreSQL" if is_postgresql else "SQLite"} syntax
2. Use proper table and column names exactly as shown in the schema, with quotes around names if needed
3. Include appropriate JOINs when needed
4. Use proper SQL functions ({"PostgreSQL" if is_postgresql else "SQLite"} compatible)
5. Be optimized for performance

Remember: ONLY return the raw SQL query with no formatting or explanation.
"""
        
        messages = [
            {"role": "system", "content": "You are an assistant that converts natural language to SQL queries. You only return the SQL query without ANY explanation or formatting."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response = self._call_mistral_api(messages)
            raw_content = response["choices"][0]["message"]["content"].strip()
            
            # Clean up the response to extract just the SQL query
            # Remove code block markers if present
            sql_query = raw_content.replace("```sql", "").replace("```", "").strip()
            
            # If there's explanatory text at the start or end, try to extract just the SQL part
            if not sql_query.lower().startswith(("select", "with")):
                # Find where the SQL query likely starts
                select_pos = sql_query.lower().find("select")
                with_pos = sql_query.lower().find("with")
                
                start_pos = -1
                if select_pos >= 0 and (with_pos == -1 or select_pos < with_pos):
                    start_pos = select_pos
                elif with_pos >= 0:
                    start_pos = with_pos
                
                if start_pos >= 0:
                    sql_query = sql_query[start_pos:].strip()
                else:
                    return "", "The generated output does not appear to be a valid SQL query"
            
            # Basic validation to ensure we got a proper SQL query
            if not sql_query.lower().startswith(("select", "with")):
                return "", "The generated output does not appear to be a valid SQL query"
            
            return sql_query, None
        except Exception as e:
            return "", str(e)
    
    def generate_narrative(self, original_query: str, sql_query: str, data: pd.DataFrame, 
                         analysis: Dict[str, Any], tone: str) -> str:
        """
        Generate narrative insights based on query results.
        
        Args:
            original_query (str): Original natural language query
            sql_query (str): Generated SQL query
            data (pd.DataFrame): Query results
            analysis (Dict[str, Any]): Data analysis results
            tone (str): Desired narrative tone (formal or casual)
            
        Returns:
            str: Generated narrative insights
        """
        # Prepare data summary for the prompt
        data_summary = f"Data shape: {data.shape[0]} rows, {data.shape[1]} columns\n"
        data_summary += f"Columns: {', '.join(data.columns.tolist())}\n\n"
        
        # Add sample data (first few rows as string)
        data_sample = data.head(5).to_string()
        
        # Add analysis insights
        analysis_text = json.dumps(analysis, indent=2)
        
        # Determine tone instructions
        tone_instructions = (
            "Use a professional, concise, and formal tone with precise language."
            if tone == "formal" else
            "Use a conversational, friendly, and easy-to-understand tone."
        )
        
        # Construct the prompt
        prompt = f"""You are a data analyst creating insights from SQL query results.

Original question: {original_query}
SQL query: {sql_query}

Data summary:
{data_summary}

Sample data:
{data_sample}

Analysis:
{analysis_text}

Generate a narrative that explains the key insights from this data in response to the original question.
{tone_instructions}
The narrative should be 3-5 paragraphs, highlighting important patterns, trends, or anomalies.
Make specific references to actual values in the data.
Do not mention that you are an AI or assistant. Simply provide the insights directly.
Format the response with appropriate Markdown headings, lists, and emphasis where helpful.
"""
        
        messages = [
            {"role": "system", "content": "You are a data analyst that provides insightful narratives from query results."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response = self._call_mistral_api(messages)
            narrative = response["choices"][0]["message"]["content"].strip()
            return narrative
        except Exception as e:
            print(f"Error generating narrative: {str(e)}")
            return f"Unable to generate insights due to an error: {str(e)}"
