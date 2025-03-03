import pandas as pd
import os
import random

# Define the base directory for CSV files
csv_dir = "Cloud/sql_files/setup"
os.makedirs(csv_dir, exist_ok=True)

# Define sample users
df_users = pd.DataFrame([
    {"id": 1, "username": "driver1", "email": "driver1@example.com", "admin": 0, "password": "hashed_pw", "activeSession": 1},
    {"id": 2, "username": "driver2", "email": "driver2@example.com", "admin": 0, "password": "hashed_pw", "activeSession": 1},
])
df_users.to_csv(os.path.join(csv_dir, "users.csv"), index=False)

# Define sample drivers
df_drivers = pd.DataFrame([
    {"id": 1, "name": "John Doe", "driverscol": "A", "DOB": "1990-05-14"},
    {"id": 2, "name": "Jane Smith", "driverscol": "B", "DOB": "1995-08-20"},
])
df_drivers.to_csv(os.path.join(csv_dir, "drivers.csv"), index=False)

# Define events
df_events = pd.DataFrame([
    {"id": 1, "name": "Race Start", "startDate": "2025-03-02 12:00:00", "endDate": "2025-03-02 14:00:00", 
     "location": "Track A", "track": "Main Track", "surfaceCondition": "Dry", "static": 0, "driver": 1, "type": "Race"},
    {"id": 2, "name": "Pit Stop", "startDate": "2025-03-02 12:45:00", "endDate": "2025-03-02 12:47:00", 
     "location": "Pit Lane", "track": "Main Track", "surfaceCondition": "Dry", "static": 1, "driver": 2, "type": "Service"},
])
df_events.to_csv(os.path.join(csv_dir, "events.csv"), index=False)

# Define car components
df_car_components = pd.DataFrame([
    {"id": 1, "semanticType": "Engine", "manufacturer": "Honda", "serialNumber": "ENG12345", "parentComponent": None},
    {"id": 2, "semanticType": "Wheel", "manufacturer": "Pirelli", "serialNumber": "WHL67890", "parentComponent": 1},
])
df_car_components.to_csv(os.path.join(csv_dir, "car_components.csv"), index=False)

# Define reading end points
df_reading_end_point = pd.DataFrame([
    {"id": 1, "name": "Engine Sensor", "functionalGroup": "Powertrain", "carComponent": 1},
    {"id": 2, "name": "Wheel Sensor", "functionalGroup": "Tire", "carComponent": 2},
])
df_reading_end_point.to_csv(os.path.join(csv_dir, "reading_end_point.csv"), index=False)

# Define sensor entities
df_sensor_entity = pd.DataFrame([
    {"id": 1001, "serialNumber": "ABC123", "purchaseDate": "2024-01-01", "sensorType": 1, "readingEndPoint": 1, "sensor_table": "sensor_1001"},
    {"id": 1002, "serialNumber": "XYZ789", "purchaseDate": "2024-02-10", "sensorType": 2, "readingEndPoint": 2, "sensor_table": "sensor_1002"},
])
df_sensor_entity.to_csv(os.path.join(csv_dir, "sensor_entity.csv"), index=False)

# Generate sensor data matching events
sensor_data = []
for sensor in df_sensor_entity.to_dict(orient='records'):
    for event in df_events.to_dict(orient='records'):
        sensor_data.append({
            "sensor_id": sensor["id"],
            "value": round(random.uniform(0.5, 100.0), 2),
            "timestamp": event["startDate"],
            "event_id": event["id"]
        })

df_sensor_data = pd.DataFrame(sensor_data)
df_sensor_data.to_csv(os.path.join(csv_dir, "sensor_data.csv"), index=False)

print(f"CSV files successfully created in: {csv_dir}")
