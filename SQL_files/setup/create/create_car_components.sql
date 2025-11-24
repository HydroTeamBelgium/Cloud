/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 */
CREATE TABLE IF NOT EXISTS car_components (
    id INT PRIMARY KEY,
    semantic_type INT NOT NULL,
    manufacturer INT,
    serial_number VARCHAR(255) UNIQUE,
    parent_component INT DEFAULT NULL,
    car_version INT,
    CONSTRAINT fk_parent_component FOREIGN KEY (parent_component)
        REFERENCES car_components(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_car_version FOREIGN KEY (car_version)
        REFERENCES car_version(id)
        ON DELETE SET NULL ON UPDATE CASCADE
);