CREATE TABLE IF NOT EXISTS {name_sensor_data_table} (
    id INT PRIMARY KEY,
    value FLOAT NOT NULL, -- only works for specific types of sensors
    -- TODO create other templates for other sensors
    timestamp DATETIME(9) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sensor_entity INT NOT NULL,
    event INT NOT NULL,
    CONSTRAINT fk_sensor_data_sensor FOREIGN KEY (sensor_entity)
    REFERENCES sensor_entity(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_data_event FOREIGN KEY (event)
    REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE
);