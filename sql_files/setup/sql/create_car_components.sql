CREATE TABLE IF NOT EXISTS car_components (
    id INT PRIMARY KEY,
    semanticType VARCHAR(45) NOT NULL,
    manufacturer VARCHAR(45),
    serialNumber VARCHAR(45) NULL, 
    parentComponent INT NULL,
    FOREIGN KEY (parentComponent) REFERENCES car_components(id) ON DELETE SET NULL
);
