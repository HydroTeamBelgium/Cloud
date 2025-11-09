/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 * Join table for sensor_type and measurement to maintain many-to-many relationship
 */
CREATE TABLE IF NOT EXISTS sensor_type_measurement_type (
    sensor_type_id INT NOT NULL,
    measurement_type_id INT NOT NULL,
    PRIMARY KEY (sensor_type_id, measurement_type_id),
    FOREIGN KEY (sensor_type_id) REFERENCES sensor_type(id) ON DELETE SET NULL ON UPDATE CASCADE,
    FOREIGN KEY (measurement_type_id) REFERENCES measurement_type(id) ON DELETE SET NULL ON UPDATE CASCADE
);