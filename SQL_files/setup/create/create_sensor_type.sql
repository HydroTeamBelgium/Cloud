CREATE TABLE IF NOT EXISTS sensor_type {
    id INT PRIMARY KEY
    manufacturer VARCHAR(255) NOT NULL,
    model VARCHAR(255) NOT NULL,
    sample_freq FLOAT NOT NULL
}