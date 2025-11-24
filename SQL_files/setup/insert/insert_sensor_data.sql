INSERT IGNORE INTO sensor_data (
    id,
    value,
    timestap,
    event,
    sensor_entity,
    measurement_type
)
VALUES (%s, %s, %s, %s, %s, %s);
