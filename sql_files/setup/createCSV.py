import pandas as pd
import os
import random
import mysql.connector
from datetime import datetime, timedelta

# Database connection details
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "Roarlol12",
    "database": "hydro_db"
}

# Define the base directory for CSV files (same as script directory)
csv_dir = os.path.abspath(os.path.dirname(__file__))
os.makedirs(csv_dir, exist_ok=True)

def fetch_sensors():
    """Fetches all sensors from the database."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, sensor_table FROM sensor_entity")
    sensors = cursor.fetchall()
    cursor.close()
    conn.close()
    return sensors

# Generate CSV files for all tables

def generate_csv_files():
    # Sample users
    df_users = pd.DataFrame([
        {"id": i, "username": f"driver{i}", "email": f"driver{i}@example.com", "admin": 0, 
         "password": "hashed_pw", "activeSession": random.choice([0, 1])}
        for i in range(1, 11)
    ])
    df_users.to_csv(os.path.join(csv_dir, "users.csv"), index=False)
    
    # Sample drivers
    df_drivers = pd.DataFrame([
        {"id": i, "name": f"Driver {i}", "driverscol": random.choice(["A", "B", "C"]), 
         "DOB": f"{random.randint(1970, 2005)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"}
        for i in range(1, 11)
    ])
    df_drivers.to_csv(os.path.join(csv_dir, "drivers.csv"), index=False)
    
    # Sample events
    df_events = pd.DataFrame([
        {"id": i, "name": random.choice(["Race Start", "Pit Stop", "Lap Completed", "Mechanical Issue", "Finish Line"]),
         "startDate": (datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S"),
         "endDate": (datetime.now() + timedelta(days=i, hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
         "location": random.choice(["Track A", "Track B", "Track C"]),
         "track": random.choice(["Main Track", "Test Circuit"]),
         "surfaceCondition": random.choice(["Dry", "Wet", "Slippery"]),
         "static": random.choice([0, 1]),
         "driver": random.randint(1, 10),
         "type": random.choice(["Race", "Service", "Test"])}
        for i in range(1, 6)
    ])
    df_events.to_csv(os.path.join(csv_dir, "events.csv"), index=False)
    
    # Sample car components with some NULL parentComponent values
    df_car_components = pd.DataFrame([
        {
            "id": i,
            "semanticType": f"Component {i}",
            "manufacturer": f"Manufacturer {i}",
            "serialNumber": f"SN-{1000+i}",
            "parentComponent": random.choice(range(1, i)) if i > 1 and random.random() > 0.3 else None  # 30% chance of NULL
        }
        for i in range(1, 11)
    ])
    df_car_components.to_csv(os.path.join(csv_dir, "car_components.csv"), index=False)

    
    # Sample reading_end_point
    df_reading_end_point = pd.DataFrame([
        {"id": i, "name": f"Endpoint {i}", "functionalGroup": f"Group {i}", "carComponent": random.randint(1, 10)}
        for i in range(1, 6)
    ])
    df_reading_end_point.to_csv(os.path.join(csv_dir, "reading_end_point.csv"), index=False)
    
    # Sample sensor_entity
    df_sensor_entity = pd.DataFrame([
        {"id": i, "serialNumber": f"SNR-{2000+i}", "purchaseDate": f"{random.randint(2015, 2023)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
         "sensorType": random.randint(1, 5), "readingEndPoint": random.randint(1, 5), "sensor_table": f"sensor_{i}"}
        for i in range(1, 6)
    ])
    df_sensor_entity.to_csv(os.path.join(csv_dir, "sensor_entity.csv"), index=False)

    print("✅ All base tables' CSV files created")
    
    # Generate sensor data
    sensors = fetch_sensors()
    for sensor in sensors:
        sensor_data = []
        for event in df_events.to_dict(orient='records'):
            for _ in range(random.randint(10, 20)):
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

if __name__ == "__main__":
    generate_csv_files()
    print(f"🎉 All CSV files successfully created in: {csv_dir}")
