
import logging
import os
from deleters.generalDeleter import delete_from_table
from constants import BASE_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def main():
    base_path = BASE_PATH
    
    try:
        # === Users ===
        delete_from_table("delete_user.sql", {"user_id": 1})

        # === Drivers ===
        delete_from_table("delete_driver.sql", {"driver_id": 1})

        # === Events ===
        delete_from_table("delete_event.sql", {"event_round": 1})

        # === Car Components ===
        delete_from_table("delete_car_component.sql", {"component_id": 1})

        # === Reading Endpoints ===
        delete_from_table("delete_reading_endpoint.sql", {"endpoint_id": 1})

        # === Sensor Entity ===
        delete_from_table("delete_sensor_entity.sql", {"sensor_id": 1})

        # === Sensor Data (dynamic table) ===
        delete_from_table("delete_sensor_data.sql", {
            "sensor_table": "sensor_1",
            "sensor_id": 1,
            "event_round": 1
        })

        logger.info("🎉 Data deleted successfully!")

    except Exception as e:
        logger.error(f"❌ Error during data delete: {e}")
        raise

if __name__ == "__main__":
    main()