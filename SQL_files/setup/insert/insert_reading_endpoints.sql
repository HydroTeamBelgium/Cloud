INSERT IGNORE INTO reading_end_point (
    id,
    name,
    car_component,
    description
)
VALUES (%s, %s, %s, %s);
