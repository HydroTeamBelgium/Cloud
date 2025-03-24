SET FOREIGN_KEY_CHECKS = 0; -- disable FK constraints (otherwise problems with truncating referenced tables)

TRUNCATE TABLE users;
TRUNCATE TABLE car_components;
TRUNCATE TABLE drivers;
TRUNCATE TABLE events;
TRUNCATE TABLE weather_sensor_data;
TRUNCATE TABLE sensor_type;
TRUNCATE TABLE dummy_sensor_data;
TRUNCATE TABLE sensor_entity;
TRUNCATE TABLE reading_end_point;

SET FOREIGN_KEY_CHECKS = 1; -- reanable FK constraints