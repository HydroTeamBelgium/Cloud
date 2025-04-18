import logging
import pandas as pd
import numpy as np
from db import connect_to_db, load_sql
from typing import List, Tuple

logger = logging.getLogger(__name__)


def get_table_metadata(database_name: str, table_name: str) -> List[Tuple[str, str]]:
    """
   

    Args:
        database_name (str): The name of the database.
        table_name (str): The name of the table.
    Returns:
        List[Tuple[str, str]]: A list of tuples containing column names and their data types.
    """
    query = load_sql("get_table_metadata.sql")

    with connect_to_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (database_name, table_name))
            results = cursor.fetchall()
            return [(row[0], row[1]) for row in results]


def infer_mysql_type(series: pd.Series) -> str:
    """
    Infers the MySQL type based on pandas dtype.
    Args:
        series (pd.Series): The pandas Series to infer the type from.
    Returns:
        str: The inferred MySQL type.
    """
    if pd.api.types.is_integer_dtype(series):
        return "int"
    elif pd.api.types.is_float_dtype(series):
        return "float"
    elif pd.api.types.is_bool_dtype(series):
        return "tinyint"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    else:
        return "varchar"


def validate_dataframe(df: pd.DataFrame, db_columns: List[Tuple[str, str]], table_name: str) -> pd.DataFrame:
    """
    Ensures that the dataframe matches the database table's column names and types.
    Logs any mismatches or warnings. Returns a corrected dataframe or None if not valid.
    
    Args:
        df (pd.DataFrame): The dataframe to validate.
        db_columns (List[Tuple[str, str]]): The database columns and their types.
        table_name (str): The name of the table.
    Returns:
        pd.DataFrame: The validated dataframe.
    Raises:
        ValueError: If the dataframe does not match the database schema.
    

    """
    db_col_names = [col for col, _ in db_columns]
    db_col_types = [typ for _, typ in db_columns]

    df_col_names = list(df.columns)
    df_col_types = [infer_mysql_type(df[col]) for col in df.columns]

    # Column count mismatch
    if len(df_col_names) != len(db_col_names):
        logger.error(f"❌ [{table_name}] Column count mismatch. CSV: {len(df_col_names)} vs DB: {len(db_col_names)}")
        return None

    # Same types (in order), but different column names → Rename
    if df_col_types == db_col_types and df_col_names != db_col_names:
        logger.warning(f"⚠️ [{table_name}] Renaming columns in dataframe to match DB schema.")
        df.columns = db_col_names
        return df

    # Same types (unordered), reorder needed
    if sorted(df_col_types) == sorted(db_col_types) and df_col_types != db_col_types:
        logger.warning(f"⚠️ [{table_name}] Reordering dataframe columns to match DB types.")
        new_order = []
        remaining = db_col_types.copy()
        for expected_type in db_col_types:
            for i, actual_type in enumerate(df_col_types):
                if actual_type == expected_type and i not in new_order:
                    new_order.append(i)
                    break
        df = df[[df.columns[i] for i in new_order]]
        return df

    # Matching column names but wrong order → reorder
    if sorted(df_col_names) == sorted(db_col_names) and df_col_types != db_col_types:
        logger.warning(f"⚠️ [{table_name}] Reordering dataframe to match DB column order.")
        df = df[db_col_names]
        return df

    # Unresolvable mismatch
    if df_col_names != db_col_names and df_col_types != db_col_types:
        logger.error(f"❌ [{table_name}] Column names and types mismatch. Cannot resolve safely.")
        return None

    return df