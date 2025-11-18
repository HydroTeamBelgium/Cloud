-- 1. Base lookup tables first
CREATE TABLE IF NOT EXISTS roles (
    id INT PRIMARY KEY,
    role INT NOT NULL
);

CREATE TABLE IF NOT EXISTS car_version (
    id INT PRIMARY KEY,
    version INT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_type (
    id INT PRIMARY KEY,
    event_type INT NOT NULL
);

CREATE TABLE IF NOT EXISTS measurement_type (
    id INT PRIMARY KEY,
    name INT NOT NULL,
    unit INT NOT NULL
);

CREATE TABLE IF NOT EXISTS sensor_type (
    id INT PRIMARY KEY,
    manufacturer INT NOT NULL,
    model VARCHAR(255) NOT NULL,
    sample_freq FLOAT NOT NULL
);

-- 2. car_components (depends on car_version)
CREATE TABLE IF NOT EXISTS car_components (
    id INT PRIMARY KEY,
    semantic_type INT NOT NULL,
    manufacturer INT NOT NULL,
    serial_number VARCHAR(255) NOT NULL UNIQUE,
    parent_component INT DEFAULT NULL,
    car_version INT,
    CONSTRAINT fk_parent_component FOREIGN KEY (parent_component)
        REFERENCES car_components(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_car_version FOREIGN KEY (car_version)
        REFERENCES car_version(id)
        ON DELETE SET NULL ON UPDATE CASCADE
);

-- 3. reading_end_point (depends on car_components)
CREATE TABLE IF NOT EXISTS reading_end_point (
    id INT PRIMARY KEY,
    name INT NOT NULL,
    car_component INT,
    description LONGTEXT,
    CONSTRAINT fk_reading_car_component FOREIGN KEY (car_component)
        REFERENCES car_components(id)
        ON DELETE SET NULL ON UPDATE CASCADE
);

-- 4. drivers (depends on roles)
CREATE TABLE IF NOT EXISTS drivers (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    dob DATE NOT NULL,
    weight INT NOT NULL,
    length INT NOT NULL,
    sex ENUM('M', 'F') NOT NULL,
    role INT,
    CONSTRAINT fk_driver_role FOREIGN KEY (role)
        REFERENCES roles(id)
        ON DELETE SET NULL ON UPDATE CASCADE
);

-- 6. events (depends on event_condition, drivers, event_type)
CREATE TABLE IF NOT EXISTS events (
    id INT PRIMARY KEY,
    name VARCHAR(45) NOT NULL,
    start_date DATETIME NOT NULL,
    end_date DATETIME NOT NULL,
    location VARCHAR(45),
    description LONGTEXT,
    track VARCHAR(45),
    static BOOLEAN DEFAULT FALSE,
    driver INT,
    event_type INT,
    CONSTRAINT fk_event_type FOREIGN KEY (event_type)
        REFERENCES event_type(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_event_driver FOREIGN KEY (driver)
        REFERENCES drivers(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
);

-- 7. sensor_entity (depends on sensor_type, reading_end_point)
CREATE TABLE IF NOT EXISTS sensor_entity (
    id INT PRIMARY KEY,
    serial_number VARCHAR(255) NOT NULL UNIQUE,
    purchase_date DATE NOT NULL,
    sensor_type INT,
    reading_end_point INT,
    CONSTRAINT fk_sensor_type FOREIGN KEY (sensor_type)
        REFERENCES sensor_type(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_reading_end_point FOREIGN KEY (reading_end_point)
        REFERENCES reading_end_point(id)
        ON DELETE SET NULL ON UPDATE CASCADE
);

-- 8. sensor_type_measurement_type (depends on sensor_type, measurement_type)
CREATE TABLE IF NOT EXISTS sensor_type_measurement_type (
    sensor_type_id INT,
    measurement_type_id INT,
    PRIMARY KEY (sensor_type_id, measurement_type_id),
    FOREIGN KEY (sensor_type_id)
        REFERENCES sensor_type(id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (measurement_type_id)
        REFERENCES measurement_type(id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- 9. sensor_data (depends on sensor_entity, measurement_type, events)
CREATE TABLE IF NOT EXISTS sensor_data (
    id INT PRIMARY KEY,
    value FLOAT NOT NULL,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event INT NOT NULL,
    sensor_entity INT,
    measurement_type INT,
    CONSTRAINT fk_measurement_type FOREIGN KEY (measurement_type)
        REFERENCES measurement_type(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_entity FOREIGN KEY (sensor_entity)
        REFERENCES sensor_entity(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_event FOREIGN KEY (event)
        REFERENCES events(id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- 10. weather_sensor_data (depends on events, sensor_entity)
CREATE TABLE IF NOT EXISTS weather_sensor_data (
    id INT PRIMARY KEY,
    precipitation_mm FLOAT,
    precipitation_type ENUM {"fog", "rain", "hail", "snow"},
    road_condition ENUM("water0", "water1", "water2", "snow", "ice"),
    wind_direction_degrees FLOAT,
    wind_strength_mps FLOAT,
    uv_index FLOAT,
    temperature FLOAT,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event INT,
    sensor_entity INT,
    CONSTRAINT fk_weather_event FOREIGN KEY (event)
        REFERENCES events(id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_weather_sensor FOREIGN KEY (sensor_entity)
        REFERENCES sensor_entity(id)
        ON DELETE SET NULL ON UPDATE CASCADE
);

-- 11. users (standalone)
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    authorisation TINYINT NOT NULL DEFAULT 0,
    password VARCHAR(45) NOT NULL,
    active_session BOOLEAN DEFAULT FALSE
);
