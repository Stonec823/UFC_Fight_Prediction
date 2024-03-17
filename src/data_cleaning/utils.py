from typing import Optional
from datetime import datetime
import pandas as pd
import numpy as np
from src.data_cleaning.constants import METHOD_MAP, WEIGHT_MAP


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize the column names: lower case, replace space with '_', remove dots."""
    df.columns = df.columns.str.lower().str.replace(' ', '_', regex=False).str.replace('.', '', regex=False)
    return df

def convert_height(height: str) -> Optional[int]:
    """Convert a height string in the format 'feet' inches'' to inches."""
    if height == '--' or not "'" in height:
        return None
    feet, inches = height.split("' ")
    feet = int(feet)
    inches = int(inches.replace('"', ''))
    return feet * 12 + inches


def convert_reach(reach: str) -> Optional[int]:
    """Convert a reach string in inches to an integer."""
    if reach == '--' or not reach.endswith('"'):
        return None
    return int(reach.replace('"', ''))

def convert_to_date(date_str: str, date_format: str = '%B %d, %Y') -> pd.Timestamp:
    """Convert a date string to a pandas Timestamp object based on the provided format."""
    if date_str == '--' or not date_str:
        return None
    try:
        return pd.to_datetime(datetime.strptime(date_str, date_format))
    except ValueError:
        return None

def extract_id(url: str) -> str:
    """Extract the last part of the URL as the ID."""
    parts = url.split('/')
    return parts[-1] if parts else ''

def split_location(location: str) -> pd.Series:
    """Split a location string into city, state (if present), and country components."""
    parts = [part.strip() for part in location.split(',')]
    
    if len(parts) == 3:  # city, state, country
        city, state, country = parts
    elif len(parts) == 2:  # city, country
        city, country = parts
        state = None  # No state available
    else:
        city = location  # Only the city is available
        state = None
        country = None

    return pd.Series([city, state, country])

def split_throw_land(df: pd.DataFrame, cols_to_split: list) -> pd.DataFrame:
    """
    Splits the values in the specified columns of a DataFrame based on ' of ' delimiter,
    creating two new columns for each original column to hold the split values.
    
    :param df: The input DataFrame.
    :param cols_to_split: A list of column names in the DataFrame to split.
    :return: The DataFrame with original columns replaced by their split components.
    """
    for col in cols_to_split:
        new_cols = [f'{col}_throw', f'{col}_land']
        df[new_cols] = df[col].str.split(' of ', expand=True)
        df.drop(col, axis=1, inplace=True)
    return df

def drop_pct_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Removes any columns that contain the pct symbol."""
    pct_cols = [col for col in df.columns if '%' in col]
    return df.drop(pct_cols, axis=1)

def split_bout(df: pd.DataFrame) -> pd.DataFrame:
    """Splits the 'bout' column into two new columns 'fighter1' and 'fighter2' based on ' vs. ' delimiter and drops the original 'bout' column."""
    df[['fighter1', 'fighter2']] = df['bout'].str.strip().str.split(' vs. ', expand=True)
    return df.drop('bout', axis=1)

def clean_outcome(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the 'outcome' column values to a numerical format where 'L/W' is replaced with 0, 
    'W/L' is replaced with 1, and all other values are replaced with None.

    :param df: The input DataFrame with an 'outcome' column containing the values to be converted.
    :return: The DataFrame with the 'outcome' column values converted to numerical format.
    """
    df['outcome'] = np.where(
        df['outcome'] == 'L/W', 0, np.where(
        df['outcome'] == 'W/L', 1, None
        ))
    return df

def clean_method(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the 'method' column by stripping whitespace and replacing values using 'METHOD_MAP'."""
    df['method'] = df['method'].str.strip().replace(METHOD_MAP)
    return df

def clean_weightclass(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the 'weightclass' column by stripping whitespace and replacing values using 'WEIGHT_MAP'."""
    df['weightclass'] = df['weightclass'].str.strip().replace(WEIGHT_MAP)
    return df


def clean_time(df: pd.DataFrame, colname: str, split_on: str = ":") -> pd.DataFrame:
    """
    Convert a time string in a DataFrame column to total seconds.
    
    Parameters:
    df (pd.DataFrame): The DataFrame containing the time strings.
    colname (str): The name of the column with the time strings to convert.
    split_on (str): The character used to split the time string into minutes and seconds.

    Returns:
    pd.DataFrame: The DataFrame with the time column converted to total seconds.
    """
    time_parts = df[colname].str.split(split_on, expand=True)
    minutes = pd.to_numeric(time_parts[0], errors='coerce')
    seconds = pd.to_numeric(time_parts[1], errors='coerce')
    # Calculate total seconds using vectorized operations
    df[colname] = minutes * 60 + seconds
    
    return df
