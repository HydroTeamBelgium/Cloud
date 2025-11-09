CREATE TABLE IF NOT EXISTS weather_sensor_data (
    id INT PRIMARY KEY,
    precipitation_mm FLOAT, -- mm/m^2
    wind_direction_degrees FLOAT, -- 0°-360°
    wind_strength_mps FLOAT, -- mps
    uv_index FLOAT,
    temperature FLOAT,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event INT NOT NULL, -- FK
    sensor_entity INT NOT NULL, -- FK
    CONSTRAINT fk_weather_event FOREIGN KEY (event)
    REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_weather_sensor FOREIGN KEY (sensor_entity)
    REFERENCES sensor_entity(id) ON DELETE SET NULL ON UPDATE CASCADE
);