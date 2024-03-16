import pandas as pd
from src.data_processing.utils import filter_event_modern_era, filter_weightclass


def fight_results_date(df_results: pd.DataFrame, df_events: pd.DataFrame, start_date: str = "2010-01-01") -> pd.DataFrame:
    """
    Filter fight results for events after a specified date and specific weight classes, and merge with event details.

    This function first filters the fight results based on specific weight classes using the `filter_weightclass` function.
    It then removes any rows with missing values in the 'outcome' column. After that, it filters the event details to include only
    events after the specified date using the `filter_event_modern_era` function with the start_date parameter. Finally, it merges 
    the filtered fight results with the filtered event details based on common columns.

    Parameters:
    - df_results (pd.DataFrame): DataFrame containing fight results, which should include weightclass and outcome columns.
    - df_events (pd.DataFrame): DataFrame containing event details, which should include a date column to filter by the start date.
    - start_date (str, optional): The start date to filter the events. Events after this date will be included. Defaults to "2010-01-01".

    Returns:
    - pd.DataFrame: A merged DataFrame containing fight results for specific weight classes and events after the specified start date.
    """
    return (
        df_results
        .pipe(filter_weightclass)
        .dropna(subset=['outcome'])
        .merge(
            df_events
            .pipe(filter_event_modern_era, start_date=start_date)
            [['event', 'date']], on='event'
            ))






    ...