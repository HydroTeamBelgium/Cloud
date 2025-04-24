 CREATE TABLE IF NOT EXISTS events (
                id INT PRIMARY KEY,
                name VARCHAR(45) NOT NULL,
                startDate DATETIME NOT NULL,
                endDate DATETIME NOT NULL,
                location VARCHAR(45),
                track VARCHAR(45),
                surfaceCondition VARCHAR(45),
                static TINYINT,
                driver INT,
                type VARCHAR(45),
                FOREIGN KEY (driver) REFERENCES drivers(id) ON DELETE SET NULL
            );