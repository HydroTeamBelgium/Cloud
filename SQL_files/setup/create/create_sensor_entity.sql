/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 */
CREATE TABLE IF NOT EXISTS sensor_entity (
    id INT PRIMARY KEY,
    serial_number VARCHAR(255) NOT NULL UNIQUE,
    purchase_date DATE NOT NULL,
    sensor_type INT NOT NULL,
    reading_end_point INT NOT NULL,
    CONSTRAINT fk_sensor_type FOREIGN KEY (sensor_type)
    REFERENCES sensor_type(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_reading_end_point FOREIGN KEY (reading_end_point)
    REFERENCES reading_end_point(id) ON DELETE SET NULL ON UPDATE CASCADE
);