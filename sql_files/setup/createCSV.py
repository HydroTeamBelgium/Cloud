import pandas as pd
import os
import random
import mysql.connector
from datetime import datetime, timedelta

# Database connection details
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "31023",
    "database": "hydro_db"
}

# Define the base directory for CSV files
csv_dir = os.path.abspath("Cloud/sql_files/setup")  # Ensure absolute path
os.makedirs(csv_dir, exist_ok=True)  # Ensure directory exists

print(f"📂 Saving CSV files to: {csv_dir}")  # Debugging info

def fetch_sensors():
    """Fetches all sensors from the database."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT id, sensor_table FROM sensor_entity")
    sensors = cursor.fetchall()

    cursor.close()
    conn.close()
    return sensors

# Sample users
df_users = pd.DataFrame([
    {"id": i, "username": f"driver{i}", "email": f"driver{i}@example.com", "admin": 0, 
     "password": "hashed_pw", "activeSession": random.choice([0, 1])}
    for i in range(1, 11)  # 10 Users
])
df_users.to_csv(os.path.join(csv_dir, "users.csv"), index=False)
print("✅ users.csv created")

# Sample drivers
df_drivers = pd.DataFrame([
    {"id": i, "name": f"Driver {i}", "driverscol": random.choice(["A", "B", "C"]), 
     "DOB": f"{random.randint(1970, 2005)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"}
    for i in range(1, 11)  # 10 Drivers
])
df_drivers.to_csv(os.path.join(csv_dir, "drivers.csv"), index=False)
print("✅ drivers.csv created")

# Sample events
df_events = pd.DataFrame([
    {"id": i, "name": random.choice(["Race Start", "Pit Stop", "Lap Completed", "Mechanical Issue", "Finish Line"]),
     "startDate": (datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S"),
     "endDate": (datetime.now() + timedelta(days=i, hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
     "location": random.choice(["Track A", "Track B", "Track C"]),
     "track": random.choice(["Main Track", "Test Circuit"]),
     "surfaceCondition": random.choice(["Dry", "Wet", "Slippery"]),
     "static": random.choice([0, 1]),
     "driver": random.randint(1, 10),  # Assign to random driver
     "type": random.choice(["Race", "Service", "Test"])}
    for i in range(1, 6)  # 5 Events
])
df_events.to_csv(os.path.join(csv_dir, "events.csv"), index=False)
print("✅ events.csv created")

# Fetch all sensors from the database
sensors = fetch_sensors()
print(f"🔍 Found {len(sensors)} sensors in database.")

# Generate sensor data for each sensor
for sensor in sensors:
    sensor_data = []
    for event in df_events.to_dict(orient='records'):
        for _ in range(random.randint(10, 20)):  # Generate 10-20 readings per event
            timestamp = datetime.strptime(event["startDate"], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(1, 120))
            sensor_data.append({
                "sensor_id": sensor["id"],
                "value": round(random.uniform(0.5, 100.0), 2),
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "event_id": event["id"]
            })

    df_sensor_data = pd.DataFrame(sensor_data)
    sensor_csv_path = os.path.join(csv_dir, f"{sensor['sensor_table']}.csv")
    df_sensor_data.to_csv(sensor_csv_path, index=False)
    print(f"✅ {sensor['sensor_table']}.csv created with {len(sensor_data)} entries")

print(f"🎉 All CSV files successfully created in: {csv_dir}")
