/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 */
CREATE TABLE IF NOT EXISTS weather_sensor_data (
    id INT PRIMARY KEY,
    precipitation_mm FLOAT,
    precipitation_type ENUM {"fog", "rain", "hail", "snow"},
    road_condition ENUM("water0", "water1", "water2", "snow", "ice"),
    wind_direction_degrees FLOAT,
    wind_strength_mps FLOAT,
    uv_index FLOAT,
    temperature FLOAT,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event INT,
    sensor_entity INT,
    CONSTRAINT fk_weather_event FOREIGN KEY (event)
        REFERENCES events(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_weather_sensor FOREIGN KEY (sensor_entity)
        REFERENCES sensor_entity(id)
        ON DELETE SET NULL ON UPDATE CASCADE
);