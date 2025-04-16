import pandas as pd
import os
import random
from datetime import datetime, timedelta
from db import fetch_sensors_from_db
from exceptions import CSVNotCreatedError
import logging

from models.user import User
from models.carComponent import CarComponent
from models.readingEndPoint import ReadingEndPoint
from models.sensorEntity import SensorEntity
from models.sensorData import SensorData

logger = logging.getLogger(__name__)

def generate_project_specific_csv_files(csv_dir):
    """
    Generates CSV files for project-specific tables using model classes.

    These tables include users, car components, reading end points, and sensor entities.
    Also generates synthetic sensor_<id>.csv files using randomized data.

    Raises:
        CSVNotCreatedError: if any expected CSV file is not created or is empty.
    """
    try:
        logger.info("Generating users.csv")
        users = [
            User(i, f"driver{i}", f"driver{i}@example.com", 0, "hashed_pw", random.choice([0, 1]))
            for i in range(1, 11)
        ]
        df_users = pd.DataFrame([user.to_dict() for user in users])
        users_csv = os.path.join(csv_dir, "users.csv")
        df_users.to_csv(users_csv, index=False)
        if not os.path.exists(users_csv) or os.path.getsize(users_csv) == 0:
            raise CSVNotCreatedError("users.csv not created or is empty")

        logger.info("Generating car_components.csv")
        components = [
            CarComponent(i, f"Component {i}", f"Manufacturer {i}", f"SN-{1000+i}",
                         random.choice(range(1, i)) if i > 1 and random.random() > 0.3 else None)
            for i in range(1, 11)
        ]
        df_components = pd.DataFrame([c.to_dict() for c in components])
        components_csv = os.path.join(csv_dir, "car_components.csv")
        df_components.to_csv(components_csv, index=False)
        if not os.path.exists(components_csv) or os.path.getsize(components_csv) == 0:
            raise CSVNotCreatedError("car_components.csv not created or is empty")

        logger.info("Generating reading_end_point.csv")
        endpoints = [
            ReadingEndPoint(i, f"Endpoint {i}", f"Group {i}", random.randint(1, 10))
            for i in range(1, 6)
        ]
        df_endpoints = pd.DataFrame([ep.to_dict() for ep in endpoints])
        endpoints_csv = os.path.join(csv_dir, "reading_end_point.csv")
        df_endpoints.to_csv(endpoints_csv, index=False)
        if not os.path.exists(endpoints_csv) or os.path.getsize(endpoints_csv) == 0:
            raise CSVNotCreatedError("reading_end_point.csv not created or is empty")

        logger.info("Generating sensor_entity.csv")
        sensors = [
            SensorEntity(i, f"SNR-{2000+i}",
                         f"{random.randint(2015, 2023)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
                         random.randint(1, 5), random.randint(1, 5), f"sensor_{i}")
            for i in range(1, 6)
        ]
        df_sensors = pd.DataFrame([s.to_dict() for s in sensors])
        sensors_csv = os.path.join(csv_dir, "sensor_entity.csv")
        df_sensors.to_csv(sensors_csv, index=False)
        if not os.path.exists(sensors_csv) or os.path.getsize(sensors_csv) == 0:
            raise CSVNotCreatedError("sensor_entity.csv not created or is empty")

        logger.info("Generating sensor_<id>.csv files")
        sensors_from_db = fetch_sensors_from_db()

        events_csv = os.path.join(csv_dir, "events.csv")
        if not os.path.exists(events_csv):
            raise CSVNotCreatedError("events.csv not found. Generate it first with generate_events_data().")
        df_events = pd.read_csv(events_csv)


        """
        Generates synthetic sensor data for testing purposes and saves it to CSV.
        
        Each sensor gets a unique CSV file with random values, timestamps, and event links.

        """


        for sensor in sensors_from_db:
            sensor_data = []
            for event in df_events.to_dict(orient='records'):
                num_samples = random.randint(10, 20)
                for _ in range(num_samples):
                    timestamp = datetime.strptime(event["date"], "%Y-%m-%d") + timedelta(minutes=random.randint(1, 120))
                    data = SensorData(sensor["id"], round(random.uniform(0.5, 100.0), 2),
                                      timestamp.strftime("%Y-%m-%d %H:%M:%S"), event["round"])
                    sensor_data.append(data.to_dict())

            df_sensor_data = pd.DataFrame(sensor_data)
            sensor_csv_path = os.path.join(csv_dir, f"{sensor['sensor_table']}.csv")
            df_sensor_data.to_csv(sensor_csv_path, index=False)
            if not os.path.exists(sensor_csv_path) or os.path.getsize(sensor_csv_path) == 0:
                raise CSVNotCreatedError(f"{sensor['sensor_table']}.csv not created or is empty")
            logger.info(f"✅ {sensor['sensor_table']}.csv created with {len(sensor_data)} entries")

        logger.info("✅ All project-specific CSV files successfully created.")

    except CSVNotCreatedError as e:
        logger.error(f"❌ Error generating project-specific CSV files: {e}")
        raise
