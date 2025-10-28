/*
 * See Notion for full documentation on DB schematic (https://www.notion.so/DB-schematic-1a0ed9807d58807cb57decf66d2e53a3?source=copy_link)
 */
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    authorisation TINYINT NOT NULL DEFAULT 0,
    password VARCHAR(45) NOT NULL,
    active_session BOOLEAN DEFAULT FALSE,
    CONSTRAINT chk_email CHECK (email like '%_@__%.__%')
);
