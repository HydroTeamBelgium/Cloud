CREATE TABLE IF NOT EXISTS reading_end_point (
    id INT PRIMARY KEY,
    name VARCHAR(45) NOT NULL,
    functionalGroup VARCHAR(45),
    carComponent INT NULL,
    FOREIGN KEY (carComponent) REFERENCES car_components(id) ON DELETE SET NULL
);
