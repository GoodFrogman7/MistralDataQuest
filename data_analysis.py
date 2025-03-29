import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple

def analyze_query_results(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze the query results to extract meaningful insights.
    
    Args:
        df (pd.DataFrame): Query results as a DataFrame
        
    Returns:
        Dict[str, Any]: Analysis results including statistics, patterns, and trends
    """
    analysis = {
        "summary": {},
        "numerical_stats": {},
        "categorical_stats": {},
        "temporal_stats": {},
        "correlations": []
    }
    
    if df.empty:
        return {"error": "No data to analyze"}
    
    # Basic dataframe information
    analysis["summary"]["row_count"] = len(df)
    analysis["summary"]["column_count"] = len(df.columns)
    analysis["summary"]["columns"] = list(df.columns)
    
    # Classify columns by data type
    numerical_columns = df.select_dtypes(include=['number']).columns.tolist()
    categorical_columns = df.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()
    
    # Try to identify date columns among object columns
    date_columns = []
    for col in df.select_dtypes(include=['object']):
        try:
            # Check if column can be converted to datetime
            pd.to_datetime(df[col], errors='raise')
            date_columns.append(col)
            # Remove identified date columns from categorical
            if col in categorical_columns:
                categorical_columns.remove(col)
        except:
            pass
    
    # Store column classifications
    analysis["summary"]["numerical_columns"] = numerical_columns
    analysis["summary"]["categorical_columns"] = categorical_columns
    analysis["summary"]["date_columns"] = date_columns
    
    # Analyze numerical columns
    for col in numerical_columns:
        try:
            col_stats = {
                "mean": float(df[col].mean()),
                "median": float(df[col].median()),
                "std": float(df[col].std()),
                "min": float(df[col].min()),
                "max": float(df[col].max()),
                "missing": int(df[col].isna().sum())
            }
            
            # Detect outliers using IQR method
            Q1 = float(df[col].quantile(0.25))
            Q3 = float(df[col].quantile(0.75))
            IQR = Q3 - Q1
            outlier_count = int(((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum())
            
            col_stats["Q1"] = Q1
            col_stats["Q3"] = Q3
            col_stats["IQR"] = IQR
            col_stats["outlier_count"] = outlier_count
            
            analysis["numerical_stats"][col] = col_stats
        except Exception as e:
            print(f"Error analyzing column {col}: {str(e)}")
    
    # Analyze categorical columns
    for col in categorical_columns:
        try:
            value_counts = df[col].value_counts().to_dict()
            # Convert keys to strings to ensure JSON serialization
            value_counts = {str(k): int(v) for k, v in value_counts.items()}
            
            col_stats = {
                "unique_values": int(df[col].nunique()),
                "missing": int(df[col].isna().sum()),
                "most_common": list(value_counts.items())[0][0] if value_counts else None,
                "most_common_count": list(value_counts.items())[0][1] if value_counts else 0,
                "value_counts": value_counts
            }
            
            analysis["categorical_stats"][col] = col_stats
        except Exception as e:
            print(f"Error analyzing column {col}: {str(e)}")
    
    # Analyze date columns
    for col in date_columns:
        try:
            dates = pd.to_datetime(df[col], errors='coerce')
            date_stats = {
                "missing": int(dates.isna().sum()),
                "min_date": dates.min().strftime('%Y-%m-%d') if not pd.isna(dates.min()) else None,
                "max_date": dates.max().strftime('%Y-%m-%d') if not pd.isna(dates.max()) else None,
                "range_days": int((dates.max() - dates.min()).days) if not pd.isna(dates.min()) and not pd.isna(dates.max()) else None
            }
            
            analysis["temporal_stats"][col] = date_stats
        except Exception as e:
            print(f"Error analyzing date column {col}: {str(e)}")
    
    # Calculate correlations between numerical columns
    if len(numerical_columns) > 1:
        try:
            corr_matrix = df[numerical_columns].corr().round(2)
            # Extract significant correlations (abs value > 0.5)
            for i in range(len(numerical_columns)):
                for j in range(i+1, len(numerical_columns)):
                    col1 = numerical_columns[i]
                    col2 = numerical_columns[j]
                    corr_value = corr_matrix.iloc[i, j]
                    if not pd.isna(corr_value) and abs(corr_value) > 0.5:
                        analysis["correlations"].append({
                            "columns": [col1, col2],
                            "correlation": float(corr_value),
                            "strength": _interpret_correlation(corr_value)
                        })
        except Exception as e:
            print(f"Error calculating correlations: {str(e)}")
    
    # Add special insights based on data patterns
    analysis["insights"] = _generate_insights(df, analysis)
    
    return analysis

def _interpret_correlation(value: float) -> str:
    """
    Interpret the strength of a correlation coefficient.
    
    Args:
        value (float): Correlation coefficient
        
    Returns:
        str: Interpretation of correlation strength
    """
    abs_value = abs(value)
    if abs_value > 0.8:
        strength = "very strong"
    elif abs_value > 0.6:
        strength = "strong"
    elif abs_value > 0.4:
        strength = "moderate"
    elif abs_value > 0.2:
        strength = "weak"
    else:
        strength = "very weak"
    
    direction = "positive" if value > 0 else "negative"
    
    return f"{strength} {direction}"

def _generate_insights(df: pd.DataFrame, analysis: Dict[str, Any]) -> List[str]:
    """
    Generate specific insights based on the data analysis.
    
    Args:
        df (pd.DataFrame): Query results
        analysis (Dict[str, Any]): Analysis results
        
    Returns:
        List[str]: List of insights
    """
    insights = []
    
    # Check for highly skewed numerical distributions
    for col, stats in analysis["numerical_stats"].items():
        if "mean" in stats and "median" in stats:
            skew_ratio = stats["mean"] / stats["median"] if stats["median"] != 0 else 0
            if abs(skew_ratio - 1) > 0.5:
                direction = "right" if skew_ratio > 1 else "left"
                insights.append(f"The distribution of {col} is highly skewed to the {direction}.")
        
        # Check for significant outliers
        if "outlier_count" in stats and stats["outlier_count"] > 0:
            outlier_percent = (stats["outlier_count"] / analysis["summary"]["row_count"]) * 100
            if outlier_percent > 5:
                insights.append(f"{col} has {stats['outlier_count']} outliers ({outlier_percent:.1f}% of data).")
    
    # Check for dominant categories
    for col, stats in analysis["categorical_stats"].items():
        if "most_common_count" in stats and "unique_values" in stats:
            dominant_percent = (stats["most_common_count"] / analysis["summary"]["row_count"]) * 100
            if dominant_percent > 70 and stats["unique_values"] > 1:
                insights.append(f"'{stats['most_common']}' dominates the {col} category at {dominant_percent:.1f}%.")
    
    # Check for strong correlations
    for corr in analysis["correlations"]:
        if abs(corr["correlation"]) > 0.7:
            insights.append(f"There is a {corr['strength']} correlation ({corr['correlation']:.2f}) between {corr['columns'][0]} and {corr['columns'][1]}.")
    
    # Look for temporal patterns if date columns exist
    for col, stats in analysis["temporal_stats"].items():
        if "range_days" in stats and stats["range_days"]:
            if stats["range_days"] > 365:
                insights.append(f"The data spans {stats['range_days'] // 365} years and {stats['range_days'] % 365} days.")
            elif stats["range_days"] > 30:
                insights.append(f"The data spans {stats['range_days'] // 30} months and {stats['range_days'] % 30} days.")
            else:
                insights.append(f"The data spans {stats['range_days']} days.")
    
    return insights
