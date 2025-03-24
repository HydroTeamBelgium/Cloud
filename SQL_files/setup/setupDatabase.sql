CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    admin TINYINT NOT NULL DEFAULT 0,
    password VARCHAR(255) NOT NULL,
    active_session BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS car_components (
    id INT PRIMARY KEY,
    semantic_type VARCHAR(255) NOT NULL,
    manufacturer VARCHAR(255) NOT NULL,
    serial_number VARCHAR(255) NOT NULL UNIQUE,
    parent_component INT DEFAULT NULL,
    CONSTRAINT fk_parent_component FOREIGN KEY (parent_component)
    REFERENCES car_components(id) ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS drivers (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    dob DATE NOT NULL,
    role ENUM('primaryDriver', 'reserveDriver') NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    start_date DATETIME NOT NULL,
    end_date DATETIME NOT NULL,
    location VARCHAR(255) NOT NULL,
    track VARCHAR(255) NOT NULL,
    surface_condition ENUM('dry', 'wet') NOT NULL,
    static BOOLEAN DEFAULT FALSE,
    driver INT NOT NULL,
    type ENUM('test', 'race', 'development') NOT NULL,
    CONSTRAINT fk_event_driver FOREIGN KEY (driver)
    REFERENCES drivers(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS weather_sensor_data (
    id BIGINT PRIMARY KEY,
    precipitation INT NOT NULL,
    wind_direction_degrees INT NOT NULL,
    wind_strength_mps INT NOT NULL,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event INT NOT NULL,
    sensor_entity INT NOT NULL,
    CONSTRAINT fk_weather_event FOREIGN KEY (event)
    REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_weather_sensor FOREIGN KEY (sensor_entity)
    REFERENCES sensor_entity(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS sensor_type (
    id INT PRIMARY KEY,
    manufacturer VARCHAR(255) NOT NULL,
    model VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    units VARCHAR(255) NOT NULL,
    sample_freq INT NOT NULL
);

CREATE TABLE IF NOT EXISTS reading_end_point (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    functional_group ENUM('engine', 'tire', 'exhaust', 'cockpit', 'aero') NOT NULL,
    car_component INT NOT NULL,
    CONSTRAINT fk_reading_car_component FOREIGN KEY (car_component)
    REFERENCES car_components(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS sensor_data (
    id INT PRIMARY KEY,
    value BIGINT NOT NULL,
    timestamp DATETIME(9) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sensor_entity INT NOT NULL,
    event INT NOT NULL,
    CONSTRAINT fk_sensor_data_sensor FOREIGN KEY (sensor_entity)
    REFERENCES sensor_entity(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_data_event FOREIGN KEY (event)
    REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS sensor_entity (
    id INT PRIMARY KEY,
    serial_number VARCHAR(255) NOT NULL UNIQUE,
    purchase_date DATE NOT NULL,
    sensor_type INT NOT NULL,
    reading_end_point INT NOT NULL,
    CONSTRAINT fk_sensor_type FOREIGN KEY (sensor_type)
    REFERENCES sensor_type(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_sensor_reading_end_point FOREIGN KEY (reading_end_point)
    REFERENCES reading_end_point(id) ON DELETE CASCADE ON UPDATE CASCADE
);