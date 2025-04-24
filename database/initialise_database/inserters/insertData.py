import logging
import os
from common.config import ConfigFactory
from common.logger import LoggerFactory
from database.inserters.InsertFactory import InsertFactory
from database.tools.utils import get_sensor_sql_filenames




def main():

    logger = LoggerFactory.get_logger(__name__)
    insert_factory = InsertFactory()
    config = ConfigFactory.load_config()
    
    try:
        # === Users ===

        insert_factory.insert_users("users")


        # === Drivers ===
        insert_factory.insert_drivers( "drivers")


        # === Events ===

        insert_factory.insert_events("events")

        # === Car Components ===
        insert_factory.insert_car_components("car_components")
        
        # === Reading Endpoints ===

        insert_factory.insert_reading_endpoints("reading_end_point")
        
        # === Sensor Data ===
        sensor_files = get_sensor_sql_filenames()
        insert_factory.insert_all_sensor_data(sensor_files)

        logger.info("🎉 All data inserted successfully!")

    except Exception as e:
        logger.error(f"❌ Error during data insertion: {e}")
        raise

if __name__ == "__main__":
    main()
