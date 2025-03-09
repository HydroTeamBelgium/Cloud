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

def ensure_parent_records():
    """Ensures all referenced parent records exist before inserting child data."""
    conn = connect_to_db()
    cursor = conn.cursor()

    # Ensure parentComponent exists in car_components before inserting
    cursor.execute("""
        INSERT IGNORE INTO car_components (id, semanticType, manufacturer, serialNumber, parentComponent)
        SELECT DISTINCT parentComponent, 'Unknown', 'Unknown', 'None', 0
        FROM car_components
        WHERE parentComponent NOT IN (SELECT id FROM car_components) AND parentComponent != 0;
    """)

    # Ensure carComponent exists in reading_end_point before inserting
    cursor.execute("""
        INSERT IGNORE INTO car_components (id, semanticType, manufacturer, serialNumber, parentComponent)
        SELECT DISTINCT carComponent, 'Unknown', 'Unknown', 'None', 0
        FROM reading_end_point
        WHERE carComponent NOT IN (SELECT id FROM car_components) AND carComponent != 0;
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Foreign key records ensured before inserting child records!")

def create_missing_sensor_tables():
    """Reads sensor IDs from sensor_entity and ensures each sensor_xxxx table exists."""
    conn = connect_to_db()
    cursor = conn.cursor()

    # Get all sensor IDs and their table names from sensor_entity
    cursor.execute("SELECT id, sensor_table FROM sensor_entity")
    sensors = cursor.fetchall()

    for sensor_id, sensor_table in sensors:
        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{sensor_table}'")
        result = cursor.fetchone()

        if not result:  # If table does not exist, create it
            create_table_query = f"""
            CREATE TABLE {sensor_table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sensor_id INT NOT NULL,
                value FLOAT NOT NULL,
                timestamp DATETIME NOT NULL,
                event_id INT NOT NULL
            );
            """
            cursor.execute(create_table_query)
            print(f"✅ Created table: {sensor_table}")
        else:
            print(f"⚡ Table {sensor_table} already exists.")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Sensor tables checked and created if necessary.")

def insert_data(csv_file, table_name, columns, columns_to_replace_null=[]):
    """Reads data from a CSV file and inserts it into the specified database table."""

    if not os.path.exists(csv_file):
        print(f"⚠️ Warning: {csv_file} not found, skipping {table_name}.")
        return

    df = pd.read_csv(csv_file)

    print(f"\n🔹 Reading data from {csv_file} for {table_name}:\n")
    print(df.head())  # Debugging: Show first few rows

    # Replace NULL with 0 in specified foreign key columns
    for col in columns_to_replace_null:
        df[col] = df[col].apply(lambda x: 0 if pd.isna(x) or x == '' else x)

    conn = connect_to_db()
    cursor = conn.cursor()

    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        values = [row[col] for col in columns]
        try:
            cursor.execute(query, tuple(values))
        except mysql.connector.errors.IntegrityError as e:
            print(f"❌ IntegrityError inserting into {table_name}: {e}")
        except mysql.connector.errors.ProgrammingError as e:
            print(f"❌ ProgrammingError inserting into {table_name}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Data successfully inserted into {table_name}!\n")

def insert_sensor_data():
    """Reads each sensor_xxxx.csv file and inserts data into the correct sensor tables."""
    sensor_files = [f for f in os.listdir(base_path) if f.startswith("sensor_") and f.endswith(".csv")]

    if not sensor_files:
        print("⚠️ Warning: No sensor CSV files found, skipping sensor data insertion.")
        return

    conn = connect_to_db()
    cursor = conn.cursor()

    for sensor_file in sensor_files:
        sensor_table = sensor_file.replace(".csv", "")  # Extract table name
        sensor_csv_path = os.path.join(base_path, sensor_file)

        print(f"🔹 Inserting data from {sensor_csv_path} into {sensor_table}...")

        df = pd.read_csv(sensor_csv_path)

        # Ensure all required columns exist
        required_columns = {"sensor_id", "value", "timestamp", "event_id"}
        if not required_columns.issubset(df.columns):
            print(f"⚠️ Skipping {sensor_csv_path} - missing columns: {required_columns - set(df.columns)}")
            continue

        df = df.where(pd.notna(df), None)  # Replace NaN with None

        query = f"INSERT IGNORE INTO {sensor_table} (sensor_id, value, timestamp, event_id) VALUES (%s, %s, %s, %s)"

        for _, row in df.iterrows():
            try:
                cursor.execute(query, (row['sensor_id'], row['value'], row['timestamp'], row['event_id']))
            except mysql.connector.errors.ProgrammingError as e:
                print(f"❌ Error inserting into {sensor_table}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Sensor data successfully inserted into all sensor tables.")

if __name__ == "__main__":
    base_path = os.path.abspath("C:/Users/dimk2/Cloud/sql_files/setup")

    # Ensure referenced records exist before inserting car_components and reading_end_point
    ensure_parent_records()

    # Table definitions with columns and which columns should replace NULL with 0
    table_definitions = {
        "users": (["id", "username", "email", "admin", "password", "activeSession"], []),
        "drivers": (["id", "name", "driverscol", "DOB"], []),
        "events": (["id", "name", "startDate", "endDate", "location", "track", "surfaceCondition", "static", "driver", "type"], []),
        "car_components": (["id", "semanticType", "manufacturer", "serialNumber", "parentComponent"], ["parentComponent"]),
        "reading_end_point": (["id", "name", "functionalGroup", "carComponent"], ["carComponent"]),
        "sensor_entity": (["id", "serialNumber", "purchaseDate", "sensorType", "readingEndPoint", "sensor_table"], ["readingEndPoint"])
    }

    # Insert all non-sensor data
    for table, (columns, columns_to_replace_null) in table_definitions.items():
        csv_file_path = os.path.join(base_path, f"{table}.csv")
        insert_data(csv_file_path, table, columns, columns_to_replace_null)

    # Create sensor tables dynamically if they don't exist
    create_missing_sensor_tables()

    # Insert sensor data into individual sensor tables
    insert_sensor_data()
