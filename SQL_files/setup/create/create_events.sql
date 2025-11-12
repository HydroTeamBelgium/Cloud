/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 */
CREATE TABLE IF NOT EXISTS events (
    id INT PRIMARY KEY,
    name VARCHAR(45) NOT NULL,
    start_date DATETIME NOT NULL,
    end_date DATETIME NOT NULL,
    location VARCHAR(45),
    description LONGTEXT,
    track VARCHAR(45),
    static BOOLEAN DEFAULT FALSE,
    event_condition INT,
    driver INT,
    event_type INT,
    CONSTRAINT fk_event_type FOREIGN KEY (event_type)
        REFERENCES event_type(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_event_driver FOREIGN KEY (driver)
        REFERENCES drivers(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_event_condition FOREIGN KEY (event_condition)
        REFERENCES event_condition(id)
        ON DELETE SET NULL ON UPDATE CASCADE
);