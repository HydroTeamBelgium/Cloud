INSERT IGNORE INTO weather_sensor_data (
    id,
    precipitation_mm,
    precipitation_type,
    road_condition,
    wind_direction_degrees,
    wind_strength_mps,
    uv_index,
    temperature,
    timestamp,
    event,
    sensor_entity
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
