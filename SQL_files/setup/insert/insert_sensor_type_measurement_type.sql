INSERT IGNORE INTO sensor_type_measurement_type (
    sensor_type_id,
    measurement_type_id
)
VALUES (%s, %s);
