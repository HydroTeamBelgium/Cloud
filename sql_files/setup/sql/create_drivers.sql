CREATE TABLE IF NOT EXISTS drivers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    given_name VARCHAR(100) NOT NULL,
    code VARCHAR(45),
    date_of_birth DATE
);
