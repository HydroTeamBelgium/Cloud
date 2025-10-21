/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 */
CREATE TABLE IF NOT EXISTS car_components (
    id INT PRIMARY KEY,
    -- semantic parameter for us, can be 'suspension', 'tire', or whatever
    semantic_type VARCHAR(255) NOT NULL,
    manufacturer VARCHAR(255) NOT NULL,
    serial_number VARCHAR(255) NOT NULL UNIQUE,
    parent_component INT DEFAULT NULL,
    CONSTRAINT fk_parent_component FOREIGN KEY (parent_component)
    REFERENCES car_components(id) ON DELETE SET NULL ON UPDATE CASCADE
);