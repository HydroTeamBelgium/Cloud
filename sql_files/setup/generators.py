import pandas as pd
import os
import random
from datetime import datetime, timedelta
from db import fetch_sensors_from_db
from exceptions import CSVNotCreatedError
import logging

logger = logging.getLogger(__name__)


def generate_project_specific_csv_files(csv_dir):
    """
    Generates CSV files for project-specific tables.

    These tables include users, car components, reading end points, and sensor entities.
    Also generates synthetic sensor_<id>.csv files using randomized data.

    Raises:
        CSVNotCreatedError: if any expected CSV file is not created or is empty.
    """
    try:
        logger.info("📦 Generating users.csv")
        df_users = pd.DataFrame([
            {"id": i, "username": f"driver{i}", "email": f"driver{i}@example.com", "admin": 0, 
             "password": "hashed_pw", "activeSession": random.choice([0, 1])}
            for i in range(1, 11)
        ])
        users_csv = os.path.join(csv_dir, "users.csv")
        df_users.to_csv(users_csv, index=False)
        if not os.path.exists(users_csv) or os.path.getsize(users_csv) == 0:
            raise CSVNotCreatedError("users.csv not created or is empty")

        logger.info("📦 Generating car_components.csv")
        df_car_components = pd.DataFrame([
            {
                "id": i,
                "semanticType": f"Component {i}",
                "manufacturer": f"Manufacturer {i}",
                "serialNumber": f"SN-{1000+i}",
                "parentComponent": random.choice(range(1, i)) if i > 1 and random.random() > 0.3 else None
            }
            for i in range(1, 11)
        ])
        components_csv = os.path.join(csv_dir, "car_components.csv")
        df_car_components.to_csv(components_csv, index=False)
        if not os.path.exists(components_csv) or os.path.getsize(components_csv) == 0:
            raise CSVNotCreatedError("car_components.csv not created or is empty")

        logger.info("📦 Generating reading_end_point.csv")
        df_reading_end_point = pd.DataFrame([
            {"id": i, "name": f"Endpoint {i}", "functionalGroup": f"Group {i}", "carComponent": random.randint(1, 10)}
            for i in range(1, 6)
        ])
        reading_csv = os.path.join(csv_dir, "reading_end_point.csv")
        df_reading_end_point.to_csv(reading_csv, index=False)
        if not os.path.exists(reading_csv) or os.path.getsize(reading_csv) == 0:
            raise CSVNotCreatedError("reading_end_point.csv not created or is empty")

        logger.info("📦 Generating sensor_entity.csv")
        df_sensor_entity = pd.DataFrame([
            {"id": i, "serialNumber": f"SNR-{2000+i}",
             "purchaseDate": f"{random.randint(2015, 2023)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
             "sensorType": random.randint(1, 5), "readingEndPoint": random.randint(1, 5), "sensor_table": f"sensor_{i}"}
            for i in range(1, 6)
        ])
        sensors_csv = os.path.join(csv_dir, "sensor_entity.csv")
        df_sensor_entity.to_csv(sensors_csv, index=False)
        if not os.path.exists(sensors_csv) or os.path.getsize(sensors_csv) == 0:
            raise CSVNotCreatedError("sensor_entity.csv not created or is empty")

        logger.info("📡 Generating sensor_<id>.csv files")
        sensors = fetch_sensors_from_db()
        
        # Load events from previously generated events.csv
        events_csv = os.path.join(csv_dir, "events.csv")
        if not os.path.exists(events_csv):
            raise CSVNotCreatedError("events.csv not found. Generate it first with generate_events_data().")
        df_events = pd.read_csv(events_csv)

        for sensor in sensors:
            sensor_data = []
            for event in df_events.to_dict(orient='records'):
                for _ in range(random.randint(10, 20)):
                    timestamp = datetime.strptime(event["date"], "%Y-%m-%d") + timedelta(minutes=random.randint(1, 120))
                    sensor_data.append({
                        "sensor_id": sensor["id"],
                        "value": round(random.uniform(0.5, 100.0), 2),
                        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        "event_id": event["round"]
                    })
            df_sensor_data = pd.DataFrame(sensor_data)
            sensor_csv_path = os.path.join(csv_dir, f"{sensor['sensor_table']}.csv")
            df_sensor_data.to_csv(sensor_csv_path, index=False)
            if not os.path.exists(sensor_csv_path) or os.path.getsize(sensor_csv_path) == 0:
                raise CSVNotCreatedError(f"{sensor['sensor_table']}.csv not created or is empty")
            logger.info(f"✅ {sensor['sensor_table']}.csv created with {len(sensor_data)} entries")

        logger.info("✅ All project-specific CSV files successfully created.")

    except Exception as e:
        logger.error(f"❌ Error generating project-specific CSV files: {e}")
        raise



