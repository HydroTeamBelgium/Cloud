CREATE TABLE IF NOT EXISTS {table_name} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sensor_id INT NOT NULL,
    value FLOAT NOT NULL,
    timestamp DATETIME NOT NULL,
    event_id INT NOT NULL
);
