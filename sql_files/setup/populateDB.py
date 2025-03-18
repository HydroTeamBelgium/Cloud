import mysql.connector
import os
import numpy as np
import pandas as pd

# Database connection details
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "Roarlol12",
    "database": "hydro_db"
}

def connect_to_db():
    """Establishes a connection to the database."""
    return mysql.connector.connect(**DB_CONFIG)

def create_tables():
    """Creates all necessary tables in the database."""
    conn = connect_to_db()
    cursor = conn.cursor()

    # Create tables
    table_definitions = {
        "users": """
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                username VARCHAR(45) NOT NULL,
                email VARCHAR(45) NOT NULL,
                admin TINYINT NOT NULL,
                password VARCHAR(45) NOT NULL,
                activeSession TINYINT NOT NULL
            )""",
        "drivers": """
            CREATE TABLE IF NOT EXISTS drivers (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                driverscol VARCHAR(45),
                DOB DATE
            )""",
        "events": """
            CREATE TABLE IF NOT EXISTS events (
                id INT PRIMARY KEY,
                name VARCHAR(45) NOT NULL,
                startDate DATETIME NOT NULL,
                endDate DATETIME NOT NULL,
                location VARCHAR(45),
                track VARCHAR(45),
                surfaceCondition VARCHAR(45),
                static TINYINT,
                driver INT,
                type VARCHAR(45),
                FOREIGN KEY (driver) REFERENCES drivers(id) ON DELETE SET NULL
            )""",

        "car_components": """
            CREATE TABLE IF NOT EXISTS car_components (
                id INT PRIMARY KEY,
                semanticType VARCHAR(45) NOT NULL,
                manufacturer VARCHAR(45),
                serialNumber VARCHAR(45) NULL, 
                parentComponent INT NULL
            )""",


        "reading_end_point": """
            CREATE TABLE IF NOT EXISTS reading_end_point (
                id INT PRIMARY KEY,
                name VARCHAR(45) NOT NULL,
                functionalGroup VARCHAR(45),
                carComponent INT NULL
            )"""
    }

    for table, query in table_definitions.items():
        cursor.execute(query)
        print(f"✅ Created table: {table}")

    # Insert data into tables before adding foreign keys
    insert_all_data()

    # Ensure referenced parentComponent and carComponent exist
    ensure_foreign_key_integrity(cursor)

    # Now add foreign keys after data insertion
    alter_foreign_keys(cursor)

    create_sensor_tables(cursor)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ All tables successfully created and foreign keys added!")

def ensure_foreign_key_integrity(cursor):
    """Ensures all referenced parentComponent and carComponent values exist."""
    cursor.execute("""
        INSERT IGNORE INTO car_components (id, semanticType, manufacturer, serialNumber, parentComponent)
        SELECT DISTINCT parentComponent, 'Unknown', 'Unknown', 'None', NULL
        FROM car_components
        WHERE parentComponent NOT IN (SELECT id FROM car_components) AND parentComponent IS NOT NULL;
    """)
    
    cursor.execute("""
        INSERT IGNORE INTO car_components (id, semanticType, manufacturer, serialNumber, parentComponent)
        SELECT DISTINCT carComponent, 'Unknown', 'Unknown', 'None', NULL
        FROM reading_end_point
        WHERE carComponent NOT IN (SELECT id FROM car_components) AND carComponent IS NOT NULL;
    """)

    cursor.execute("CREATE TEMPORARY TABLE temp_ids (id INT)")
    cursor.execute("INSERT INTO temp_ids (id) SELECT id FROM car_components")
    cursor.execute("""UPDATE car_components SET parentComponent = id WHERE parentComponent NOT IN (SELECT id FROM temp_ids) OR parentComponent IS NULL""")
    cursor.execute("DROP TEMPORARY TABLE temp_ids")


    print("✅ Ensured all referenced IDs exist in car_components.")

def alter_foreign_keys(cursor):
    """Adds foreign key constraints only if they do not already exist."""
    
    # Check if the foreign key constraint already exists
    cursor.execute("SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_NAME = 'car_components' AND CONSTRAINT_NAME = 'fk_parentComponent'")
    parent_fk_exists = cursor.fetchone()

    if not parent_fk_exists:
        cursor.execute("""
            ALTER TABLE car_components 
            ADD CONSTRAINT fk_parentComponent FOREIGN KEY (parentComponent) 
            REFERENCES car_components(id) ON DELETE SET NULL
        """)
        print("✅ Foreign key fk_parentComponent added!")

    cursor.execute("SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_NAME = 'reading_end_point' AND CONSTRAINT_NAME = 'fk_carComponent'")
    car_component_fk_exists = cursor.fetchone()

    if not car_component_fk_exists:
        cursor.execute("""
            ALTER TABLE reading_end_point 
            ADD CONSTRAINT fk_carComponent FOREIGN KEY (carComponent) 
            REFERENCES car_components(id) ON DELETE SET NULL
        """)
        print("✅ Foreign key fk_carComponent added!")

    print("✅ Foreign keys checked and added if necessary!")

def create_sensor_tables(cursor):
    """Creates sensor tables dynamically based on available sensor CSV files."""
    base_path = os.path.abspath(os.path.dirname(__file__))
    sensor_files = [f for f in os.listdir(base_path) if f.startswith("sensor_") and f.endswith(".csv")]

    for sensor_file in sensor_files:
        table_name = sensor_file.replace(".csv", "")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sensor_id INT NOT NULL,
            value FLOAT NOT NULL,
            timestamp DATETIME NOT NULL,
            event_id INT NOT NULL
        )"""
        cursor.execute(create_table_query)
        print(f"✅ Created sensor table: {table_name}")


def insert_data(csv_file, table_name, columns):
    """Reads data from a CSV file and inserts it into the specified database table dynamically, skipping NaN values."""
    if not os.path.exists(csv_file):
        print(f"⚠️ Warning: {csv_file} not found, skipping {table_name}.")
        return

    df = pd.read_csv(csv_file)

    # Convert all NaN values to None
    df = df.replace({np.nan: None})

    conn = connect_to_db()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        # Remove NaN (None) values dynamically from each row
        non_null_columns = [col for col in columns if row[col] is not None]
        values = [row[col] for col in non_null_columns]

        # If the entire row is NaN, skip inserting it
        if not values:
            continue

        placeholders = ', '.join(['%s'] * len(non_null_columns))
        query = f"INSERT INTO {table_name} ({', '.join(non_null_columns)}) VALUES ({placeholders})"

        try:
            cursor.execute(query, tuple(values))
        except mysql.connector.errors.IntegrityError as e:
            print(f"❌ IntegrityError inserting into {table_name}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Data inserted into {table_name}!")


def insert_all_data():
    """Inserts data into tables before adding foreign keys."""
    base_path = os.path.abspath(os.path.dirname(__file__))
    
    table_definitions = {
        "users": ["id", "username", "email", "admin", "password", "activeSession"],
        "drivers": ["id", "name", "driverscol", "DOB"],
        "events": ["id", "name", "startDate", "endDate", "location", "track", "surfaceCondition", "static", "driver", "type"],
        "car_components": ["id", "semanticType", "manufacturer", "serialNumber", "parentComponent"],
        "reading_end_point": ["id", "name", "functionalGroup", "carComponent"]
    }
    
    for table, columns in table_definitions.items():
        csv_file_path = os.path.join(base_path, f"{table}.csv")
        insert_data(csv_file_path, table, columns)

def insert_sensor_data():
    """Inserts data into sensor tables dynamically."""
    base_path = os.path.abspath(os.path.dirname(__file__))
    sensor_files = [f for f in os.listdir(base_path) if f.startswith("sensor_") and f.endswith(".csv")]

    conn = connect_to_db()
    cursor = conn.cursor()

    for sensor_file in sensor_files:
        table_name = sensor_file.replace(".csv", "")
        csv_file_path = os.path.join(base_path, sensor_file)

        # Load CSV
        df = pd.read_csv(csv_file_path)
        
        # Debug: Print column names if the expected column is missing
        expected_columns = {"sensor_id", "value", "timestamp", "event_id"}
        actual_columns = set(df.columns)

        if not expected_columns.issubset(actual_columns):
            print(f"⚠️ Warning: {table_name}.csv is missing expected columns!")
            print(f"🔍 Available columns: {list(df.columns)}")
            continue  # Skip this file if columns don't match

        # Ensure the correct column names (case sensitivity issues)
        df.rename(columns=lambda x: x.strip().lower(), inplace=True)
        column_mapping = {
            "id": "sensor_id",  # If 'id' is used instead of 'sensor_id'
            "timestamp": "timestamp",  # Ensure timestamp is correct
            "value": "value",
            "event_id": "event_id"
        }

        df.rename(columns=column_mapping, inplace=True)

        # Insert data
        query = f"INSERT INTO {table_name} (sensor_id, value, timestamp, event_id) VALUES (%s, %s, %s, %s)"
        for _, row in df.iterrows():
            cursor.execute(query, (row['sensor_id'], row['value'], row['timestamp'], row['event_id']))

        print(f"✅ Data inserted into {table_name}")

    conn.commit()
    cursor.close()
    conn.close()



def main():
    """Main execution function."""
    create_tables()
    insert_sensor_data()
    print("🎉 Database successfully populated!")

if __name__ == "__main__":
    main()
# This script creates the tables in the database and populates them with data from CSV files. It also ensures that foreign key constraints are added after the data has been inserted. The script uses the pandas library to read data from CSV files and the mysql.connector library to interact with the MySQL database.