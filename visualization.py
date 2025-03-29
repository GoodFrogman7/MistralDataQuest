import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Any

def create_visualization(df: pd.DataFrame, query: str) -> go.Figure:
    """
    Create an appropriate visualization based on the query results.
    
    Args:
        df (pd.DataFrame): Query results
        query (str): Original query in natural language
        
    Returns:
        go.Figure: Plotly figure object with the visualization
    """
    # If dataframe is empty or has only one row, return empty figure with message
    if df.empty or len(df) <= 1:
        fig = go.Figure()
        fig.add_annotation(
            text="Not enough data to visualize",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        return fig
    
    # Classify columns
    numerical_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()
    
    # Try to identify date columns
    date_cols = []
    for col in categorical_cols:
        try:
            pd.to_datetime(df[col])
            date_cols.append(col)
        except:
            pass
    
    # Remove identified date columns from categorical
    categorical_cols = [col for col in categorical_cols if col not in date_cols]
    
    # Determine visualization type based on data structure and query content
    query = query.lower()
    
    # Check if query contains comparison keywords
    comparison_keywords = ["compare", "comparison", "versus", "vs", "against", "difference", "distribution"]
    is_comparison = any(keyword in query for keyword in comparison_keywords)
    
    # Check if query contains time-related keywords
    time_keywords = ["time", "year", "month", "day", "date", "trend", "growth", "decline", "increase", "decrease"]
    is_time_series = any(keyword in query for keyword in time_keywords) and date_cols
    
    # Check if query contains relationship keywords
    relationship_keywords = ["relation", "relationship", "correlation", "affect", "impact", "influence", "between"]
    is_relationship = any(keyword in query for keyword in relationship_keywords) and len(numerical_cols) >= 2
    
    # Determine visualization type based on data structure and keywords
    if is_time_series and date_cols and numerical_cols:
        return create_time_series(df, date_cols[0], numerical_cols, categorical_cols)
    elif is_comparison and categorical_cols and numerical_cols:
        return create_comparison(df, categorical_cols[0], numerical_cols[0])
    elif is_relationship and len(numerical_cols) >= 2:
        return create_scatter_plot(df, numerical_cols[0], numerical_cols[1], categorical_cols[0] if categorical_cols else None)
    elif len(numerical_cols) >= 1 and len(categorical_cols) >= 1:
        return create_bar_chart(df, categorical_cols[0], numerical_cols[0])
    elif len(numerical_cols) >= 2:
        return create_scatter_plot(df, numerical_cols[0], numerical_cols[1], None)
    elif len(categorical_cols) >= 1:
        return create_pie_chart(df, categorical_cols[0])
    elif len(numerical_cols) >= 1:
        return create_histogram(df, numerical_cols[0])
    else:
        # Fallback to a table view if no appropriate visualization
        fig = go.Figure()
        fig.add_annotation(
            text="No appropriate visualization available for this data",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16)
        )
        return fig

def create_bar_chart(df: pd.DataFrame, category_col: str, value_col: str) -> go.Figure:
    """
    Create a bar chart visualization.
    
    Args:
        df (pd.DataFrame): Query results
        category_col (str): Column to use for categories
        value_col (str): Column to use for values
        
    Returns:
        go.Figure: Bar chart figure
    """
    # Aggregate data if there are too many categories
    if df[category_col].nunique() > 10:
        # Sort by value and get top 10
        agg_df = df.groupby(category_col)[value_col].sum().reset_index().sort_values(value_col, ascending=False).head(10)
        title = f"Top 10 {category_col} by {value_col}"
    else:
        agg_df = df.groupby(category_col)[value_col].sum().reset_index()
        title = f"{value_col} by {category_col}"
    
    # Sort values for better visualization
    agg_df = agg_df.sort_values(value_col)
    
    fig = px.bar(
        agg_df, 
        x=category_col, 
        y=value_col,
        title=title,
        labels={category_col: category_col.replace('_', ' ').title(), value_col: value_col.replace('_', ' ').title()},
        color_discrete_sequence=['#636EFA'],
        text=value_col
    )
    
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    
    return fig

def create_pie_chart(df: pd.DataFrame, category_col: str) -> go.Figure:
    """
    Create a pie chart visualization.
    
    Args:
        df (pd.DataFrame): Query results
        category_col (str): Column to use for categories
        
    Returns:
        go.Figure: Pie chart figure
    """
    # Count occurrences of each category
    value_counts = df[category_col].value_counts()
    
    # If there are too many categories, keep top ones and group others
    if len(value_counts) > 8:
        top_categories = value_counts.head(7)
        others_count = value_counts[7:].sum()
        
        # Create a new series with top categories and "Others"
        values = pd.Series(list(top_categories) + [others_count])
        labels = list(top_categories.index) + ["Others"]
    else:
        values = value_counts.values
        labels = value_counts.index
    
    fig = px.pie(
        values=values,
        names=labels,
        title=f"Distribution of {category_col}",
        labels={category_col: category_col.replace('_', ' ').title()},
        hole=0.4,
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    
    return fig

def create_histogram(df: pd.DataFrame, value_col: str) -> go.Figure:
    """
    Create a histogram visualization.
    
    Args:
        df (pd.DataFrame): Query results
        value_col (str): Column to create histogram for
        
    Returns:
        go.Figure: Histogram figure
    """
    fig = px.histogram(
        df, 
        x=value_col,
        title=f"Distribution of {value_col}",
        labels={value_col: value_col.replace('_', ' ').title()},
        color_discrete_sequence=['#636EFA'],
        nbins=20
    )
    
    fig.update_layout(bargap=0.05)
    
    return fig

def create_scatter_plot(df: pd.DataFrame, x_col: str, y_col: str, color_col: str = None) -> go.Figure:
    """
    Create a scatter plot visualization.
    
    Args:
        df (pd.DataFrame): Query results
        x_col (str): Column to use for x-axis
        y_col (str): Column to use for y-axis
        color_col (str, optional): Column to use for color coding
        
    Returns:
        go.Figure: Scatter plot figure
    """
    if color_col and df[color_col].nunique() <= 10:
        fig = px.scatter(
            df, 
            x=x_col, 
            y=y_col,
            color=color_col,
            title=f"Relationship between {x_col} and {y_col}",
            labels={
                x_col: x_col.replace('_', ' ').title(),
                y_col: y_col.replace('_', ' ').title(),
                color_col: color_col.replace('_', ' ').title() if color_col else None
            },
            opacity=0.7,
            size_max=10
        )
    else:
        fig = px.scatter(
            df, 
            x=x_col, 
            y=y_col,
            title=f"Relationship between {x_col} and {y_col}",
            labels={
                x_col: x_col.replace('_', ' ').title(),
                y_col: y_col.replace('_', ' ').title()
            },
            opacity=0.7,
            color_discrete_sequence=['#636EFA'],
            size_max=10
        )
    
    # Add trendline
    try:
        fig.update_layout(shapes=[{
            'type': 'line',
            'x0': df[x_col].min(),
            'y0': df[y_col].min(),
            'x1': df[x_col].max(),
            'y1': df[y_col].max(),
            'line': {
                'color': 'rgba(255, 0, 0, 0.5)',
                'width': 1,
                'dash': 'dot'
            }
        }])
    except:
        pass
    
    return fig

def create_time_series(df: pd.DataFrame, date_col: str, value_cols: List[str], category_col: str = None) -> go.Figure:
    """
    Create a time series visualization.
    
    Args:
        df (pd.DataFrame): Query results
        date_col (str): Column with dates
        value_cols (List[str]): Columns with values to plot
        category_col (str, optional): Column to use for grouping
        
    Returns:
        go.Figure: Time series figure
    """
    # Convert to datetime if not already
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Sort by date
    df = df.sort_values(date_col)
    
    # Choose one numeric column to plot
    value_col = value_cols[0]
    
    if category_col and df[category_col].nunique() <= 5:
        # Create a grouped time series
        fig = px.line(
            df, 
            x=date_col, 
            y=value_col,
            color=category_col,
            title=f"{value_col} over time by {category_col}",
            labels={
                date_col: date_col.replace('_', ' ').title(),
                value_col: value_col.replace('_', ' ').title(),
                category_col: category_col.replace('_', ' ').title()
            },
            markers=True
        )
    else:
        # Create a simple time series
        fig = px.line(
            df, 
            x=date_col, 
            y=value_col,
            title=f"{value_col} over time",
            labels={
                date_col: date_col.replace('_', ' ').title(),
                value_col: value_col.replace('_', ' ').title()
            },
            markers=True
        )
    
    fig.update_xaxes(rangeslider_visible=True)
    
    return fig

def create_comparison(df: pd.DataFrame, category_col: str, value_col: str) -> go.Figure:
    """
    Create a comparison visualization (bar chart or box plot).
    
    Args:
        df (pd.DataFrame): Query results
        category_col (str): Column to use for categories
        value_col (str): Column to use for values
        
    Returns:
        go.Figure: Comparison figure
    """
    # Check if the category has many unique values
    if df[category_col].nunique() <= 2:
        # For binary categories, use a grouped bar chart
        agg_df = df.groupby(category_col)[value_col].agg(['mean', 'min', 'max']).reset_index()
        
        fig = go.Figure()
        
        for category in agg_df[category_col]:
            row = agg_df[agg_df[category_col] == category].iloc[0]
            fig.add_trace(go.Bar(
                x=["Mean", "Minimum", "Maximum"],
                y=[row["mean"], row["min"], row["max"]],
                name=str(category),
                text=[f"{row['mean']:.2f}", f"{row['min']:.2f}", f"{row['max']:.2f}"],
                textposition='auto'
            ))
        
        fig.update_layout(
            title=f"Comparison of {value_col} by {category_col}",
            xaxis_title="Statistic",
            yaxis_title=value_col.replace('_', ' ').title(),
            barmode='group'
        )
    else:
        # For multiple categories, use a box plot
        fig = px.box(
            df, 
            x=category_col, 
            y=value_col,
            title=f"Distribution of {value_col} by {category_col}",
            labels={
                category_col: category_col.replace('_', ' ').title(),
                value_col: value_col.replace('_', ' ').title()
            },
            color=category_col,
            points="all"
        )
    
    return fig
