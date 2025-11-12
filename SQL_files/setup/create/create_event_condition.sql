/*
 * is used if there is no weather_sensor_data available
 */
CREATE TABLE IF NOT EXISTS event_condition (
    id INT PRIMARY KEY,
    precipitation_mm FLOAT,
    wind_direction_degrees INT,
    wind_strength_mps FLOAT,
    surface_contamination ENUM('water0', 'water1', 'water2', 'snow', 'ice'),
    uv_index INT,
    temperature FLOAT,
    CONSTRAINT chk_uv_index CHECK (uv_index >= 0 AND uv_index <= 12),
    CONSTRAINT chk_wind_direction_degrees CHECK (wind_direction_degrees >= 0 AND wind_direction_degrees <= 359)
);