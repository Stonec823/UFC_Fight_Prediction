import pandas as pd
from src.data_cleaning.utils import (
    clean_column_names,
    convert_height, 
    convert_reach,
    convert_to_date,
    extract_id,
    split_location,
    split_throw_land,
    drop_pct_cols,
    clean_outcome, 
    clean_method,
    clean_time,
    clean_weightclass,
    split_bout
    )

def clean_fighter_tott(df: pd.DataFrame) -> pd.DataFrame:
    """
    file: ufc_fighter_tott.csv

    Cleans the fighter data by standardizing column names, converting height, reach, and DOB,
    extracting fighter ID from URL, and dropping unnecessary columns.
    
    :param df: The input dataframe containing fighter details.
    :return: A cleaned dataframe with standardized and extracted information.
    """
    new_df = df.copy(deep=True).pipe(clean_column_names)
    
    if 'weight' in new_df.columns:
        new_df.drop('weight', axis=1, inplace=True)
    
    if 'height' in new_df.columns:
        new_df['height'] = new_df['height'].apply(convert_height)
    
    if 'reach' in new_df.columns:
        new_df['reach'] = new_df['reach'].apply(convert_reach)
    
    if 'dob' in new_df.columns:
        new_df['dob'] = new_df['dob'].apply(convert_to_date)
    
    if 'url' in new_df.columns:
        new_df['fighter_id'] = new_df['url'].apply(extract_id)
        new_df.drop('url', axis=1, inplace=True)
    return new_df

def clean_event_details(df: pd.DataFrame) -> pd.DataFrame:
    """
    file: ufc_event_details.csv

    Cleans event details in a dataframe by standardizing column names to lowercase,
    extracting event IDs from URLs, converting date strings to datetime objects,
    and splitting location strings into city, state, and country.
    
    :param df: The input dataframe containing event details.
    :return: A cleaned dataframe with standardized and processed information.
    """
    new_df = df.copy(deep=True).pipe(clean_column_names)
        
    if 'url' in new_df.columns:
        new_df['event_id'] = new_df['url'].apply(extract_id)
        new_df.drop('url', axis=1, inplace=True)
        
    if 'date' in new_df.columns:
        new_df['date'] = new_df['date'].apply(convert_to_date, date_format='%B %d, %Y')
    
    if 'location' in new_df.columns:
        new_df[['city', 'state', 'country']] = new_df['location'].apply(split_location)
    return new_df

def clean_fighter_details(df: pd.DataFrame) -> pd.DataFrame:
    """
    file: ufc_fighter_details.csv

    Cleans fighter details in a dataframe by standardizing column names to lowercase
    and extracting fighter IDs from URLs. The URL column is then dropped.
    
    :param df: The input dataframe containing fighter details.
    :return: A cleaned dataframe with standardized column names and extracted fighter IDs.
    """
    new_df = df.copy(deep=True).pipe(clean_column_names)
        
    if 'url' in new_df.columns:
        new_df['fighter_id'] = new_df['url'].apply(extract_id)
        new_df.drop('url', axis=1, inplace=True)
    return new_df

def clean_fight_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    file: ufc_fight_stats.csv
    
    Cleans fight statistics data by standardizing column names, splitting specified columns
    based on a delimiter, and dropping percentage columns.

    This function applies a pipeline of operations to the input DataFrame:
    1. Standardizes column names by converting them to lower case, replacing spaces with underscores,
       and removing dots.
    2. Splits the values in specified columns (like 'sigstr', 'total_str', etc.) based on ' of ' delimiter,
       creating two new columns for each original column to represent 'thrown' and 'landed' stats.
    3. Drops any columns that contain percentages.

    :param df: The input DataFrame containing fight statistics.
    :return: A DataFrame with cleaned and processed fight statistics.
    """
    cols_to_spit = ['sigstr', 'total_str', 'td', 'head', 'body', 'leg', 'distance', 'clinch', 'ground']
    return (
        df
        .pipe(clean_column_names)
        .pipe(split_throw_land, cols_to_spit)
        .pipe(drop_pct_cols))

def clean_fight_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    file: ufc_fight_results.csv

    Cleans the fight results data by applying a series of cleaning functions, extracting fight ID,
    and dropping specific columns.

    The function standardizes column names, extracts an ID from the 'url' column, cleans outcome
    and method columns, splits the 'bout' column into two separate columns, converts the 'time'
    column to total seconds, and drops unnecessary columns.

    :param df: The input DataFrame with fight results data.
    :return: A cleaned DataFrame with standardized and relevant information for fight analysis.
    """
    to_drop = ['time_format', 'referee', 'details', 'url']
    new_df = df.copy(deep=True).pipe(clean_column_names)
    new_df['fight_id'] = new_df['url'].apply(extract_id)
    return (
        new_df
        .pipe(clean_outcome)
        .pipe(clean_method)
        .pipe(clean_weightclass)
        .pipe(split_bout)
        .pipe(clean_time, 'time')
        .drop(to_drop, axis=1)
    )
