from dataclasses import asdict
from datetime import datetime, timedelta
import logging, os
import numpy as np
import pandas as pd
from typing import Any

from common.Singleton import SingletonMeta
from common.config import ConfigFactory
from common.logger import LoggerFactory
from common.exceptions import CSVNotCreatedError

from database.models.car_component import CarComponent
from database.models.car_version import CarVersion
from database.models.reading_end_point import ReadingEndPoint
from database.models.sensor_data import SensorData
from database.models.sensor_entity import SensorEntity
from database.models.sensor_type import SensorType
from database.models.measurement_type import MeasurementType
from database.models.sensor_type_measurement_type import SensorTypeMeasurementType
from database.models.users import Users
from database.models.roles import Roles
from database.models.drivers import Driver, Sex
from database.models.events import Event
from database.models.event_type import EventType
from database.models.weather_sensor_data import WeatherSensorData, precipitationType, roadCondition

DRIVERS = 10
ROLES = 3

LOCATIONS = 5 # See https://www.notion.so/Table-events-location-mapping-2b0ed9807d588064b6efce3fc55a1181
EVENTS = 10
EVENT_CONDITIONS = 3
EVENT_TYPES = 3 # See https://www.notion.so/Table-event_type-event_type-mapping-2aeed9807d588019bbd7d91f8607599a
WEATHER_SENSOR_DATA_PER_EVENT = 5

USERS = 10
AUTHORISATIONS = 3

CAR_VERSIONS = 2
CAR_COMPONENTS = 10
CAR_COMPONENTS_SEMANTIC_TYPES = 5 # See https://www.notion.so/Table-car_components-semantic_type-mapping-2aeed9807d5880eaacebcc80b8c9481f?pvs=25
MANUFACTURERS = 3 # See https://www.notion.so/Table-car_components-sensor_type-manufacturer-mapping-2aeed9807d58808b9c14d54a9aaa53dd
                  # used for both car_components and sensor_type

READING_END_POINTS = 4 # See https://www.notion.so/Table-reading_end_point-name-mapping-2aeed9807d58807ba480f2b526edffb0
SENSOR_TYPES = 4
MEASUREMENT_TYPES = 6 # See https://www.notion.so/Table-measurement_type-name-mapping-2afed9807d588001a8eee7a5142023e9
MEASUREMENT_TYPES_UNITS = 3 # See https://www.notion.so/Table-measurement_type-unit-mapping-2afed9807d58808e966ad06762087322
SENSOR_ENTITIES = 6
SENSOR_DATA_PER_EVENT = 20

class DataFactory(metaclass = SingletonMeta):

    _logger: logging.Logger
    _csv_dir: str

    def __init__(self):
        self._logger = LoggerFactory().get_logger(__name__)
        self._csv_dir = ConfigFactory().load_config().get_config_param("csv_files")["location"]
        os.makedirs(self._csv_dir, exist_ok=True)
    
    def generate_project_specific_csv_files(self) -> None:
        """
        Generates CSV files for project-specific tables using model classes.

        Creates: `roles.csv`, `drivers.csv`, `event_type.csv`, `events.csv`,
        `users.csv`, `car_version.csv`, `car_components.csv`, `reading_end_point.csv`,
        `sensor_type.csv`, `measurement_type.csv`, `sensor_type_measurement_type.csv`,
        `sensor_entity.csv`, `weather_sensor_data.csv`, and `sensor_data.csv`.

        Raises:
            CSVNotCreatedError: If any expected CSV file is not created or is empty.
        """
        try:
            # database = Database()
            self._logger.info("Generating roles.csv")
            roles = [
                Roles(
                    id=i,
                    role=i
                )
                for i in range(1, ROLES + 1)
            ]
            self._dataclass_list_to_csv(roles, "roles.csv")

            self._logger.info("Generating drivers.csv")
            drivers = [
                Driver(
                    id=index,
                    name=f"Driver {index}",
                    dob=datetime(1990 + index, 1, 1),
                    role=np.random.randint(1, ROLES + 1),
                    weight=70 + index,
                    height=170 + index,
                    sex=np.random.choice(Sex._member_names_)
                )
                for index in range(1, DRIVERS + 1)
            ]
            self._dataclass_list_to_csv(drivers, "drivers.csv")

            self._logger.info("Generating event_type.csv")
            event_types = [
                EventType(
                    id=index,
                    event_type=index # See https://www.notion.so/Table-event_type-event_type-mapping-2aeed9807d588019bbd7d91f8607599a?source=copy_link
                ) for index in range(1, EVENT_TYPES + 1)
            ]
            self._dataclass_list_to_csv(event_types, "event_type.csv")

            self._logger.info("Generating events.csv")
            events = [
                Event(
                    id=index,
                    name=f"Event {index}",
                    start_date=datetime(2024 + index, 1, 1, 9, 0, 0),
                    end_date=datetime(2024 + index, 1, 2, 9, 0, 0),
                    location=np.random.randint(1, LOCATIONS + 1),
                    description=f"Description {index}",
                    track=f"Track {index}",
                    static=np.random.choice([True, False]),
                    driver=np.random.randint(1, DRIVERS + 1),
                    event_type=np.random.randint(1, EVENT_TYPES + 1)
                )
                for index in range(1, EVENTS + 1)
            ]
            self._dataclass_list_to_csv(events, "events.csv")

            self._logger.info("Generating users.csv")
            users = [
                Users(
                    id=i,
                    username=f"user{i}",
                    email=f"user{i}@hydroteam.be",
                    authorisation=np.random.randint(1, AUTHORISATIONS + 1),
                    password="hashed_pw",
                    active_session=np.random.choice([False, True])
                )
                for i in range(1, USERS + 1)
            ]
            self._dataclass_list_to_csv(users, "users.csv")
            
            self._logger.info("Generating car_version.csv")
            car_versions = [
                CarVersion(
                    id=i,
                    version=i
                )
                for i in range(1, CAR_VERSIONS + 1)
            ]
            self._dataclass_list_to_csv(car_versions, "car_version.csv")

            self._logger.info("Generating car_components.csv")
            components = [
                CarComponent(
                    id=i,
                    semantic_type=np.random.randint(1, CAR_COMPONENTS_SEMANTIC_TYPES + 1),
                    manufacturer=np.random.randint(1, MANUFACTURERS + 1),
                    serial_number=f"SN-{1000+i}",
                    parent_component=np.random.choice([None] + list(range(1, i))) if i > 1 else None,
                    car_version=CAR_VERSIONS # Assign all components to the same car version for simplicity
                )
                for i in range(1, CAR_COMPONENTS + 1)
            ]
            self._dataclass_list_to_csv(components, "car_components.csv")

            self._logger.info("Generating reading_end_point.csv")
            endpoints = [
                ReadingEndPoint(
                    id=i,
                    name=np.random.randint(1, READING_END_POINTS + 1),
                    car_component=np.random.randint(1, CAR_COMPONENTS + 1),
                    description=f"Description for Endpoint {i}"
                )
                for i in range(1, READING_END_POINTS + 1)
            ]
            self._dataclass_list_to_csv(endpoints, "reading_end_point.csv")

            self._logger.info("Generating sensor_type.csv")
            sensor_types = [
                SensorType(
                    id=i,
                    manufacturer=np.random.randint(1, MANUFACTURERS + 1),
                    model=f"Model {i}",
                    sample_freq=i*10,
                )
                for i in range(1, SENSOR_TYPES + 1)
            ]
            self._dataclass_list_to_csv(sensor_types, "sensor_type.csv")

            self._logger.info("Generating measurement_type.csv")
            measurement_types = [
                MeasurementType(
                    id=i,
                    name=i,
                    unit=np.random.randint(1, MEASUREMENT_TYPES_UNITS + 1)
                )
                for i in range(1, MEASUREMENT_TYPES + 1)
            ]
            self._dataclass_list_to_csv(measurement_types, "measurement_type.csv")
            
            self._logger.info("Generating sensor_type_measurement_type.csv")
            sensor_type_measurement_types = [
                SensorTypeMeasurementType(
                    sensor_type_id=st_id,
                    measurement_type_id=np.random.randint(1, MEASUREMENT_TYPES + 1)
                )
                for st_id in range(1, SENSOR_TYPES + 1) # make sure each sensor type has a measurement type
            ]
            self._dataclass_list_to_csv(sensor_type_measurement_types, "sensor_type_measurement_type.csv")

            self._logger.info("Generating sensor_entity.csv")
            sensor_entities = [
                SensorEntity(
                    id=i,
                    serial_number=f"SNR-{2000+i}",
                    purchase_date=datetime(np.random.randint(2015, 2023), np.random.randint(1, 12), np.random.randint(1, 28)),
                    sensor_type=np.random.randint(1, SENSOR_TYPES + 1),
                    reading_end_point=np.random.randint(1, READING_END_POINTS + 1)
                )
                for i in range(1, SENSOR_ENTITIES + 1)
            ]
            self._dataclass_list_to_csv(sensor_entities, "sensor_entity.csv")

            self._logger.info("Generating weather_sensor_data.csv")
            weather_sensor_data = []
            current_id = 0
            for event in events:
                for _ in range(WEATHER_SENSOR_DATA_PER_EVENT):
                    current_id += 1

                    data = WeatherSensorData(
                        id=current_id,
                        precipitation_mm=round(np.random.uniform(0.0, 50.0), 2),
                        precipitation_type=np.random.choice(precipitationType._member_names_),
                        road_condition=np.random.choice(roadCondition._member_names_),
                        wind_direction_degrees=round(np.random.uniform(0, 360), 2),
                        wind_strength_mps=round(np.random.uniform(0.0, 20.0), 2),
                        uv_index=round(np.random.uniform(0.0, 11.0), 2),
                        temperature=round(np.random.uniform(15.0, 35.0), 2),
                        timestamp=(event.start_date + timedelta(minutes=np.random.randint(1, 120))),
                        event=event.id,
                        sensor_entity=np.random.choice([sensor.id for sensor in sensor_entities])
                    )
                    weather_sensor_data.append(data)

            self._dataclass_list_to_csv(weather_sensor_data, "weather_sensor_data.csv")


            self._logger.info("Generating sensor_data.csv")
            sensor_data = []
            current_id = 0
            for event in events:
                for sensor in sensor_entities:
                    for _ in range(SENSOR_DATA_PER_EVENT):
                        current_id += 1

                        data = SensorData(
                            id=current_id, # or event_index * 10_000 + sensor_index * 100 + sample_index + 1,
                            value=round(np.random.uniform(0.5, 100.0), 2),
                            timestamp=(event.start_date + timedelta(minutes=np.random.randint(1, 120))),
                            event=event.id,
                            sensor_entity=sensor.id,
                            measurement_type=[i for i in sensor_type_measurement_types if i.sensor_type_id == sensor.sensor_type][0].measurement_type_id
                            # each sensor type has (at least one) measurement type
                            # pick the first one
                        )
                        sensor_data.append(data)

            self._dataclass_list_to_csv(sensor_data, "sensor_data.csv")

            self._logger.info(f"✅ sensor_data.csv created with {len(sensor_data)} entries")
            self._logger.info("✅ All project-specific CSV files successfully created.")

        except CSVNotCreatedError as e:
            self._logger.error(f"❌ Error generating project-specific CSV files: {e}")
            raise

    def _dataclass_list_to_csv(self, dataclass_list: list[Any], filename: str) -> None:
        """
        Converts a list of dataclass instances to a CSV file.

        Args:
            dataclass_list (list[Any]): List of dataclass instances.
            filename (str): The name of the CSV file to write the data to.

        Raises:
            CSVNotCreatedError: If the CSV file is not created or is empty.
        """
        # when there is a column with int and None, need to use convert_dtypes()! Otherwise pandas infers the column as float
        df = pd.DataFrame([asdict(dc) for dc in dataclass_list]).convert_dtypes()
        
        file_path = os.path.join(self._csv_dir, filename)
        df.to_csv(file_path, index=False)
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            raise CSVNotCreatedError(f"{file_path} not created or is empty")