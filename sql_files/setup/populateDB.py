import pandas as pd
import mysql.connector
import os

# Database connection details
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "31023",
    "database": "hydro_db"
}

def connect_to_db():
    """Establishes a connection to the database."""
    return mysql.connector.connect(**DB_CONFIG)

def insert_data(csv_file, table_name, columns):
    """Reads data from a CSV file and inserts it into the specified database table."""
    df = pd.read_csv(csv_file)

    # Ensure NaN values are properly handled
    df.fillna(0, inplace=True)

    # Convert parentComponent to integer if it exists
    if "parentComponent" in df.columns:
        df["parentComponent"] = df["parentComponent"].apply(lambda x: int(x) if pd.notna(x) else 0)

    conn = connect_to_db()
    cursor = conn.cursor()

    placeholders = ', '.join(['%s'] * len(columns))
    query = f"""
    INSERT IGNORE INTO {table_name} ({', '.join(columns)}) 
    VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        try:
            print(f"Inserting row into {table_name}: {tuple(row[col] for col in columns)}")  # Debugging
            cursor.execute(query, tuple(row[col] for col in columns))
        except mysql.connector.errors.ProgrammingError as e:
            print(f"Error inserting into {table_name}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data successfully inserted into {table_name}!")

def insert_sensor_data(csv_file):
    """Dynamically inserts data into individual sensor tables."""
    df = pd.read_csv(csv_file)
    df.fillna(0, inplace=True)

    conn = connect_to_db()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        table_name = f"sensor_{row['sensor_id']}"  # Sensor-specific table
        query = f"INSERT IGNORE INTO {table_name} (value, timestamp, event) VALUES (%s, %s, %s)"
        try:
            cursor.execute(query, (row['value'], row['timestamp'], row['event_id']))
        except mysql.connector.errors.ProgrammingError as e:
            print(f"Error inserting into {table_name}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("Sensor data successfully inserted into individual sensor tables!")

if __name__ == "__main__":
    base_path = "Cloud/sql_files/setup"

    tables = {
        "users": ["id", "username", "email", "admin", "password", "activeSession"],
        "drivers": ["id", "name", "driverscol", "DOB"],
        "events": ["id", "name", "startDate", "endDate", "location", "track", "surfaceCondition", "static", "driver", "type"],
        "car_components": ["id", "semanticType", "manufacturer", "serialNumber", "parentComponent"],  # Replaces NaN with 0
        "reading_end_point": ["id", "name", "functionalGroup", "carComponent"],
        "sensor_entity": ["id", "serialNumber", "purchaseDate", "sensorType", "readingEndPoint", "sensor_table"]
    }

    for table, columns in tables.items():
        csv_file_path = os.path.join(base_path, f"{table}.csv")
        if os.path.exists(csv_file_path):
            insert_data(csv_file_path, table, columns)
        else:
            print(f"Warning: {csv_file_path} not found, skipping {table}.")

    # Insert data into individual sensor tables (FIXED)
    sensor_data_file = os.path.join(base_path, "sensor_data.csv")
    if os.path.exists(sensor_data_file):
        insert_sensor_data(sensor_data_file)  # ✅ Corrected function call
    else:
        print("Warning: sensor_data.csv not found, skipping sensor data insertion.")
