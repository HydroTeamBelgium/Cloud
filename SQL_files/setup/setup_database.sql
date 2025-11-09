CREATE TABLE IF NOT EXISTS car_components (
    id INT PRIMARY KEY,
    -- semantic parameter for us, can be 'suspension', 'tire', or whatever
    semantic_type VARCHAR(255) NOT NULL,
    manufacturer VARCHAR(255) NOT NULL,
    serial_number VARCHAR(255) NOT NULL UNIQUE,
    parent_component INT DEFAULT NULL,
    car_version INT, -- FK
    CONSTRAINT fk_parent_component FOREIGN KEY (parent_component)
    REFERENCES car_components(id) ON DELETE SET NULL ON UPDATE CASCADE
    CONSTRAINT fk_car_version FOREIGN KEY (car_version)
    REFERENCES car_version(id) ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS car_version (
    id INT PRIMARY KEY, 
    version VARCHAR(45) -- e.g. 1.0.0, 2.0.1, ...
);

CREATE TABLE IF NOT EXISTS drivers (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    dob DATE NOT NULL, -- dob = Date of birth
    role INT NOT NULL,
    CONSTRAINT fk_driver_role FOREIGN KEY (role),
    REFERENCES roles(id) ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS event_condition {
    id INT PRIMARY KEY
    precipitation_mm FLOAT, -- mm/m^2
    wind_direction_degrees INT, -- 0°-360°
    wind_strength_mps FLOAT, -- mps
    surface_contamination ENUM('water0', 'water1', 'water2', 'snow', 'ice'),
    uv_index INT,
    temperature FLOAT,
    CONSTRAINT chk_uv_index CHECK (uv_index >= 0 AND uv_index <= 12)
    CONSTRAINT chk_wind_direction_degrees CHECK (wind_direction_degrees >= 0 AND wind_direction_degrees <= 359)
}


CREATE TABLE IF NOT EXISTS event_type (
    id INT PRIMARY KEY,
    event_type VARCHAR(255)
);

 CREATE TABLE IF NOT EXISTS events (
    id INT PRIMARY KEY,
    name VARCHAR(45) NOT NULL,
    start_date DATETIME NOT NULL,
    end_date DATETIME NOT NULL,
    location VARCHAR(45),
    description LONGTEXT,
    track VARCHAR(45),
    static BOOLEAN DEFAULT FALSE,
    event_condition INT, -- FK
    driver INT, -- FK
    event_type INT NOT NULL, -- FK
    CONSTRAINT fk_event_type FOREIGN KEY (event_type)
    REFERENCES event_type(id) ON DELETE SET NULL ON UPDATE CASCADE
    CONSTRAINT fk_event_driver FOREIGN KEY (driver)
    REFERENCES drivers(id) ON DELETE SET NULL ON UPDATE CASCADE
    CONSTRAINT fk_event_condition FOREIGN KEY (event_condition)
    REFERENCES event_condition(id) ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS sensor_type_measurement_type (
    sensor_type_id INT NOT NULL,
    measurement_type_id INT NOT NULL,
    PRIMARY KEY (sensor_type_id, measurement_type_id),
    FOREIGN KEY (sensor_type_id) REFERENCES sensor_type(id) ON DELETE SET NULL ON UPDATE CASCADE,
    FOREIGN KEY (measurement_type_id) REFERENCES measurement_type(id) ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS measurement_type (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,      -- e.g. 'temperature', 'humidity'
    unit VARCHAR(50) NOT NULL       -- e.g. '°C', '%' 
);

CREATE TABLE IF NOT EXISTS reading_end_point (
    id INT PRIMARY KEY,
    name VARCHAR(45) NOT NULL,
    car_component INT NOT NULL,
    description LONGTEXT, -- semantical description (eg. 'front right brake disk')
    CONSTRAINT fk_reading_car_component FOREIGN KEY (car_component),
    REFERENCES car_components(id) ON DELETE SET NULL ON UPDATE CASCADE
);


CREATE TABLE IF NOT EXISTS roles (
    id INT PRIMARY KEY,
    role VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS sensor_data (
    id INT PRIMARY KEY,
    value FLOAT NOT NULL,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event INT NOT NULL,
    sensor_entity INT NOT NULL, -- FK
    measurement_type INT NOT NULL, -- FK
    CONSTRAINT fk_measurement_type FOREIGN KEY (measurement_type)
    REFERENCES measurement_type(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_entity FOREIGN KEY (sensor_entity)
    REFERENCES sensor_entity(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_event FOREIGN KEY (event)
    REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS sensor_entity (
    id INT PRIMARY KEY,
    serial_number VARCHAR(255) NOT NULL UNIQUE,
    purchase_date DATE NOT NULL,
    sensor_type INT NOT NULL,
    reading_end_point INT NOT NULL,
    CONSTRAINT fk_sensor_type FOREIGN KEY (sensor_type)
    REFERENCES sensor_type(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_reading_end_point FOREIGN KEY (reading_end_point)
    REFERENCES reading_end_point(id) ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS sensor_type_measurement_type (
    sensor_type_id INT NOT NULL,
    measurement_type_id INT NOT NULL,
    is_primary BOOLEAN DEFAULT FALSE,
    min_expected_value DECIMAL(12,4) DEFAULT NULL,
    max_expected_value DECIMAL(12,4) DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sensor_type_id, measurement_type_id),
    CONSTRAINT fk_stmt_sensor_type FOREIGN KEY (sensor_type_id)
    REFERENCES sensor_type(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_stmt_measurement_type FOREIGN KEY (measurement_type_id)
    REFERENCES measurement_type(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS sensor_type (
    id INT PRIMARY KEY,
    manufacturer VARCHAR(255) NOT NULL,
    model VARCHAR(255) NOT NULL,
    sample_freq FLOAT NOT NULL,
);

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    authorisation TINYINT NOT NULL DEFAULT 0,
    password VARCHAR(45) NOT NULL,
    active_session BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS weather_sensor_data (
    id INT PRIMARY KEY,
    precipitation_mm FLOAT, -- mm/m^2
    wind_direction_degrees FLOAT, -- 0°-360°
    wind_strength_mps FLOAT, -- mps
    uv_index FLOAT,
    temperature FLOAT,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event INT, -- FK
    sensor_entity INT NOT NULL, -- FK
    CONSTRAINT fk_weather_event FOREIGN KEY (event)
    REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_weather_sensor FOREIGN KEY (sensor_entity)
    REFERENCES sensor_entity(id) ON DELETE SET NULL ON UPDATE CASCADE
);