INSERT IGNORE INTO sensor_type (
    id,
    manufacturer,
    model,
    sample_freq
)
VALUES (%s, %s, %s, %s);
