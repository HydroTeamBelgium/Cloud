CREATE TABLE IF NOT EXISTS sensor_type_measurement_type (
    sensor_type_id INT NOT NULL,
    measurement_type_id INT NOT NULL,
    is_primary BOOLEAN DEFAULT FALSE,
    min_expected_value DECIMAL(12,4) DEFAULT NULL,
    max_expected_value DECIMAL(12,4) DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sensor_type_id, measurement_type_id),
    CONSTRAINT fk_stmt_sensor_type FOREIGN KEY (sensor_type_id)
    REFERENCES sensor_type(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_stmt_measurement_type FOREIGN KEY (measurement_type_id)
    REFERENCES measurement_type(id) ON DELETE CASCADE ON UPDATE CASCADE
);