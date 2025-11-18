INSERT IGNORE INTO sensor_entity (
    id,
    serial_number,
    purchase_date,
    sensor_type,
    reading_end_point
)
VALUES (%s, %s, %s, %s, %s);
