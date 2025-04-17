from db import connect_to_db, load_sql
import logging
logger = logging.getLogger(__name__)

def create_and_seed_sensor_entity():
    """
    Creates the sensor_entity table and inserts predefined sensors into it.
    """
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                # Create the table
                create_sql = load_sql("create_sensor_entity.sql")
                cursor.execute(create_sql)
                
                # Insert rows
                insert_sql = load_sql("insert_sensor_entities.sql")
                sensor_rows = [
                    (1, "sensor_1"),
                    (2, "sensor_2"),
                    (3, "sensor_3"),
                ]
                cursor.executemany(insert_sql, sensor_rows)

            conn.commit()
            logger.info("✅ sensor_entity table created and populated.")
    except Exception as e:
        logger.error(f"❌ Failed to create or insert into sensor_entity: {e}")
        raise
