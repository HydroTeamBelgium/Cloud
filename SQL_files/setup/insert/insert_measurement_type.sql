INSERT IGNORE INTO measurement_type (
    id,
    name,
    unit
)
VALUES (%s, %s, %s);
