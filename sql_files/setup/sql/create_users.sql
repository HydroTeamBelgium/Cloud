CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    username VARCHAR(45) NOT NULL,
    email VARCHAR(45) NOT NULL,
    admin TINYINT NOT NULL,
    password VARCHAR(45) NOT NULL,
    activeSession TINYINT NOT NULL
);
