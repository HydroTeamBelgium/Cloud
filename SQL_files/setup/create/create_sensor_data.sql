/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 */
CREATE TABLE IF NOT EXISTS sensor_data (
    id INT PRIMARY KEY,
    value FLOAT NOT NULL,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event INT NOT NULL,
    sensor_entity INT,
    measurement_type INT,
    CONSTRAINT fk_measurement_type FOREIGN KEY (measurement_type)
        REFERENCES measurement_type(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_entity FOREIGN KEY (sensor_entity)
        REFERENCES sensor_entity(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_event FOREIGN KEY (event)
        REFERENCES events(id)
        ON DELETE CASCADE ON UPDATE CASCADE
);