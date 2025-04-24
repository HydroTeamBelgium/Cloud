INSERT IGNORE INTO car_components (
    id,
    semanticType,
    manufacturer,
    serialNumber,
    parentComponent
)
VALUES (%s, %s, %s, %s, %s);
