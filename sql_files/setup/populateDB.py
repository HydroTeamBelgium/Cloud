
import os

import logging
from db import load_sql, connect_to_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLE_ORDER = [
    "users",
    "drivers",
    "car_components",
    "reading_end_point",
    "events"
]







def create_tables():
    """
    Creates all tables using prewritten SQL files (including foreign keys).

    Executes in a strict order to preserve FK dependency integrity.

    The order of table creation is defined in the TABLE_ORDER list.

    Throws an exception if any table creation fails.
    """
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                for table in TABLE_ORDER:
                    sql = load_sql(f"create_{table}.sql")
                    cursor.execute(sql)
                    logger.info(f"✅ Created table: {table}")

                conn.commit()
                logger.info("✅ All main tables created with foreign keys intact.")
    except Exception as e:
        logger.error(f"❌ Error creating tables: {e}")
        raise

def create_sensor_tables():
    """
    Creates all sensor_<id> tables based on detected CSV files,

    excluding 'sensor_entity.csv'.

    Loads `create_sensor_table.sql` template and replaces `{table_name}`.

    The script assumes that the CSV files are named in the format `sensor_<id>.csv`.
    
    """
    base_path = os.path.abspath(os.path.dirname(__file__))
    sensor_files = [
        f for f in os.listdir(base_path)
        if f.startswith("sensor_") and f.endswith(".csv") and f != "sensor_entity.csv"
    ]

    try:
        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                template = load_sql("create_sensor_table.sql")

                for f in sensor_files:
                    table_name = f.replace(".csv", "")
                    query = template.replace("{table_name}", table_name)
                    cursor.execute(query)
                    logger.info(f"✅ Created sensor table: {table_name}")

                conn.commit()
    except Exception as e:
        logger.error(f"❌ Failed to create sensor tables: {e}")
        raise


def main():
    """Main execution function."""
    create_tables()
    create_sensor_tables()
    logger.info("🎉 Database successfully populated!")

if __name__ == "__main__":
    main()
# This script creates the tables in the database and populates them with data from CSV files. It also ensures that foreign key constraints are added after the data has been inserted. The script uses the pandas library to read data from CSV files and the mysql.connector library to interact with the MySQL database.