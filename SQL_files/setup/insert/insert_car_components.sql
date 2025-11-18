INSERT IGNORE INTO car_components (
    id,
    semantic_type,
    manufacturer,
    serial_number,
    parent_component,
    car_version
)
VALUES (%s, %s, %s, %s, %s, %s);
