import csv
import os
import logging
from typing import List

from models.carComponent import CarComponent
from db import connect_to_db, load_sql

logger = logging.getLogger(__name__)

def load_car_components_from_csv(csv_path: str) -> List[CarComponent]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    components = []
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            logger.info(f"CSV Headers: {reader.fieldnames}")  # <--- Add this
            for row in reader:
                try:
                    component = CarComponent(
                        id=int(row["id"]),
                        semantic_type=row["semantic_type"],  # fixed key name
                        manufacturer=row["manufacturer"] or None,
                        serial_number=row["serial_number"] or None,
                        parent_component=int(float(row["parent_component"])) if row.get("parent_component") else None
                    )
                    components.append(component)
                except Exception as e:
                    logger.info(
                        f"Inserting row: id={row['id']}, semantic_type={row.get('semanticType')}, "
                        f"manufacturer={row.get('manufacturer')}, serial_number={row.get('serialNumber')}, "
                        f"parent_component={row.get('parentComponent')}"
                    )
                    logger.warning(f"⚠️ Skipping row due to error: {e}")
    except Exception as e:
        logger.error(f"❌ Failed to load car components: {e}")
        raise

    return components

def insert_car_components(components: List[CarComponent]):
    if not components:
        logger.info("No car components to insert.")
        return

    try:
        sql_template = load_sql("insert_car_components.sql")

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                for comp in components:
                    try:
                        cursor.execute(sql_template, (
                            comp.id,
                            comp.semantic_type,
                            comp.manufacturer,
                            comp.serial_number,
                            comp.parent_component
                        ))
                    except Exception as e:
                        logger.error(f"❌ Failed to insert car component ID={comp.id}: {e}")
                        raise
            conn.commit()
            logger.info(f"✅ Inserted {len(components)} car components into database.")
    except Exception as e:
        logger.error(f"❌ Error during car component insertion: {e}")
        raise
