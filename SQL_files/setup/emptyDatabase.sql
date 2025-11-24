SET FOREIGN_KEY_CHECKS = 0; -- disable FK constraints (otherwise problems with truncating referenced tables)

TRUNCATE TABLE users;
TRUNCATE TABLE weather_sensor_data;
TRUNCATE TABLE sensor_data;
TRUNCATE TABLE sensor_type_measurement_type;
TRUNCATE TABLE sensor_entity;
TRUNCATE TABLE events;
TRUNCATE TABLE drivers;
TRUNCATE TABLE reading_end_point;
TRUNCATE TABLE car_components;
TRUNCATE TABLE sensor_type;
TRUNCATE TABLE measurement_type;
TRUNCATE TABLE event_type;
TRUNCATE TABLE car_version;
TRUNCATE TABLE roles;

SET FOREIGN_KEY_CHECKS = 1; -- reanable FK constraints