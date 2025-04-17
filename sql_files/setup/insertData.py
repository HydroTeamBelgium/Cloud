import logging
import os
from inserters.user_inserter import load_users_from_csv, insert_users
from inserters.driver_inserter import load_drivers_from_csv, insert_drivers
from inserters.event_inserter import load_events_from_csv, insert_events
from inserters.car_component_inserter import load_car_components_from_csv, insert_car_components
from inserters.reading_endpoint_inserter import load_reading_endpoints_from_csv, insert_reading_endpoints
from inserters.sensor_data_inserter import insert_all_sensor_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    base_path = os.path.abspath(os.path.dirname(__file__))
    
    try:
        # === Users ===
        user_csv = os.path.join(base_path, "users.csv")
        users = load_users_from_csv(user_csv)
        insert_users(users)


        # === Drivers ===
        driver_csv = os.path.join(base_path, "drivers.csv")
        drivers = load_drivers_from_csv(driver_csv)
        insert_drivers(drivers)

        # === Events ===
        event_csv = os.path.join(base_path, "events.csv")
        events = load_events_from_csv(event_csv)
        insert_events(events)

        # === Car Components ===
        car_csv = os.path.join(base_path, "car_components.csv")
        car_components = load_car_components_from_csv(car_csv)
        insert_car_components(car_components)
        
        # === Reading Endpoints ===
        reading_csv = os.path.join(base_path, "reading_end_point.csv")
        endpoints = load_reading_endpoints_from_csv(reading_csv)
        insert_reading_endpoints(endpoints)
        
        # === Sensor Data ===
        base_path = os.path.abspath(os.path.dirname(__file__))
        sensor_files = [
            f for f in os.listdir(base_path)
            if f.startswith("sensor_") and f.endswith(".csv") and f != "sensor_entity.csv"
        ]
        insert_all_sensor_data(sensor_files,base_path)

        logger.info("🎉 All data inserted successfully!")

    except Exception as e:
        logger.error(f"❌ Error during data insertion: {e}")
        raise

if __name__ == "__main__":
    main()
