INSERT IGNORE INTO reading_end_point (
    id,
    name,
    functionalGroup,
    carComponent
)
VALUES (%s, %s, %s, %s);
