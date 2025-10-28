/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 */
CREATE TABLE IF NOT EXISTS measurement_type (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,      -- e.g. 'temperature', 'humidity'
    unit VARCHAR(50) NOT NULL,       -- e.g. '°C', '%' 
);