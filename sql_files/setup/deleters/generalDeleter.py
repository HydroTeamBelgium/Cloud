import os
from db import connect_to_db, load_sql
import logging

logger = logging.getLogger(__name__)

def delete_from_table(sql_file: str, values: dict):
    """
    
    Deletes records from a table using a SQL file and values to replace placeholders.
    
    Args:
        sql_file (str): The name of the SQL file containing the delete statement.
        values (dict): A dictionary of values to replace in the SQL statement.
        
    Raises:
        Exception: If there is an error during the deletion process.
            
    """
    try:
        query = load_sql(sql_file)
        for key, val in values.items():
            query = query.replace(f"{{{key}}}", str(val))

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()
            logger.info(f"✅ Deleted record using {sql_file} with {values}")

    except Exception as e:
        logger.error(f"❌ Failed to delete using {sql_file} with {values}: {e}")
        raise
