/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 * ReadingEndPoint objects differ from CarComponent objects as follows (example):
 * A sensor can be placed on brake disk with serial number ABC123, the reading_end_point would e.g. be front right brake disk,
 * the car_component would be brake disc ABC123.
 * The reading_end_point would thus not be an actual fysical entity.
 */

CREATE TABLE IF NOT EXISTS reading_end_point (
    id INT PRIMARY KEY,
    name VARCHAR(45) NOT NULL,
    car_component INT,
    description LONGTEXT,
    CONSTRAINT fk_reading_car_component FOREIGN KEY (car_component)
        REFERENCES car_components(id)
        ON DELETE SET NULL ON UPDATE CASCADE
);