CREATE TABLE IF NOT EXISTS dummy_sensor_data (
    id INT PRIMARY KEY,
    value BIGINT NOT NULL, -- This is specific for the sensor type, could also be 3 fields (e.g. 'x-axis', 'y-axis', 'z-axis')
    timestamp DATETIME(9) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sensor_entity INT NOT NULL,
    event INT NOT NULL,
    CONSTRAINT fk_sensor_data_sensor FOREIGN KEY (sensor_entity)
    REFERENCES sensor_entity(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_data_event FOREIGN KEY (event)
    REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE
);