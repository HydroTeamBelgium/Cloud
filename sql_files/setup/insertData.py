import logging
import os
from db import connect_to_db, load_sql
from typing import List
from inserters.user_inserter import load_users_from_csv, insert_users
from inserters.driver_inserter import load_drivers_from_csv, insert_drivers
from inserters.event_inserter import load_events_from_csv, insert_events
from inserters.car_component_inserter import load_car_components_from_csv, insert_car_components
from inserters.reading_endpoint_inserter import load_reading_endpoints_from_csv, insert_reading_endpoints
from inserters.sensor_data_inserter import insert_all_sensor_data
from constants import BASE_PATH


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def get_existing_tables(database_name: str) -> List[str]:
    """
    Loads existing table names from the current database schema.

    Args:
        database_name (str): The name of the database.

    Returns:
        List[str]: A list of table names.
    """
    query = load_sql("get_existing_tables.sql")
    
    with connect_to_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (database_name,))
            results = cursor.fetchall()
            return [row[0] for row in results]



def main():
    base_path = BASE_PATH

    try:
        database_name = "hydro_db"  # ← REPLACE with your real DB name
        tables = get_existing_tables(database_name)

        if "users" in tables:
            user_csv = os.path.join(base_path, "users.csv")
            users = load_users_from_csv(user_csv)
            insert_users(users)

        if "drivers" in tables:
            driver_csv = os.path.join(base_path, "drivers.csv")
            drivers = load_drivers_from_csv(driver_csv)
            insert_drivers(drivers)

        if "events" in tables:
            event_csv = os.path.join(base_path, "events.csv")
            events = load_events_from_csv(event_csv)
            insert_events(events)

        if "car_components" in tables:
            car_csv = os.path.join(base_path, "car_components.csv")
            car_components = load_car_components_from_csv(car_csv)
            insert_car_components(car_components)

        if "reading_end_point" in tables:
            reading_csv = os.path.join(base_path, "reading_end_point.csv")
            endpoints = load_reading_endpoints_from_csv(reading_csv)
            insert_reading_endpoints(endpoints)

        # Sensor tables (match pattern)
        for table in tables:
            if table.startswith("sensor_") and table != "sensor_entity":
                sensor_csv_path = os.path.join(base_path, f"{table}.csv")
                insert_all_sensor_data([f"{table}.csv"], base_path)

        logger.info("🎉 All data inserted successfully!")

    except Exception as e:
        logger.error(f"❌ Error during data insertion: {e}")
        raise

if __name__ == "__main__":
    main()
