import pandas as pd
import numpy as np
from typing import List


from src.data_processing.constants import WEIGHT_CLASSES


def filter_weightclass(df: pd.DataFrame, weightclass_column_name: str = 'weightclass', weightclass: List[str] = WEIGHT_CLASSES) -> pd.DataFrame:
    """
    Filter a DataFrame based on specified weight classes.

    This function filters rows in a DataFrame where the value in the specified weight class column matches
    one of the values in the provided list of weight classes. It converts the weight class column to a
    categorical type to improve performance.

    Parameters:
    - df (pd.DataFrame): The input DataFrame to be filtered.
    - weightclass_column_name (str, optional): The name of the column containing weight class information. Defaults to 'weightclass'.
    - weightclass (List[str], optional): A list of weight classes to filter by. Defaults to a predefined list of weight classes.

    Returns:
    - pd.DataFrame: A DataFrame containing only the rows where the weight class matches one of the specified classes.
    """
    df[weightclass_column_name] = df[weightclass_column_name].astype('category')
    return df[df[weightclass_column_name].isin(weightclass)]


def filter_event_modern_era(df: pd.DataFrame, start_date: str="2010-01-01") -> pd.DataFrame:
    """
    Filter the DataFrame to include only events after January 1, 2010.

    This function filters the input DataFrame to return only the rows where the event date is
    after January 1, 2010, considering these as part of the 'modern era' of UFC.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing UFC event details.

    Returns:
    - pd.DataFrame: A DataFrame filtered to include only events from the modern era.
    """
    # Ensure the 'date' column is in datetime format
    if not np.issubdtype(df['date'].dtype, np.datetime64):
        df['date'] = pd.to_datetime(df['date'])
    
    # Filter the DataFrame based on the date
    return df[df['date'] > start_date]
