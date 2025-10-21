/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 */
CREATE TABLE IF NOT EXISTS drivers (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    dob DATE NOT NULL, -- dob = Date of birth
    role INT NOT NULL,
    CONSTRAINT fk_driver_role FOREIGN KEY (role),
    REFERENCES roles(id) ON DELETE SET NULL ON UPDATE CASCADE
);
