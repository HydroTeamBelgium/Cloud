import logging
import os
from inserters.user_inserter import load_users_from_csv, insert_users
from inserters.driver_inserter import load_drivers_from_csv, insert_drivers

# Optional: import more insert functions as you implement them
# from models.car_component import load_components_from_csv, insert_car_components
# from models.driver import load_drivers_from_csv, insert_drivers
# ...

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    base_path = os.path.abspath(os.path.dirname(__file__))
    
    try:
        # === Users ===
        user_csv = os.path.join(base_path, "users.csv")
        users = load_users_from_csv(user_csv)
        insert_users(users)

        logger.info("✅ Users inserted successfully!")

        # === Drivers ===
        driver_csv = os.path.join(base_path, "drivers.csv")
        drivers = load_drivers_from_csv(driver_csv)
        insert_drivers(drivers)

        # === Add more here in the same pattern ===
        # car_components = load_components_from_csv(...)
        # insert_car_components(car_components)

        # drivers = load_drivers_from_csv(...)
        # insert_drivers(drivers)

        logger.info("🎉 All data inserted successfully!")

    except Exception as e:
        logger.error(f"❌ Error during data insertion: {e}")
        raise

if __name__ == "__main__":
    main()
