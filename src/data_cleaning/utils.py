from typing import Optional
from datetime import datetime
import pandas as pd


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize the column names: lower case, replace space with '_', remove dots."""
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('.', '')
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