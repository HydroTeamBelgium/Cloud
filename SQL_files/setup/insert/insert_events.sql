INSERT IGNORE INTO events (
    id,
    name,
    start_date,
    end_date,
    location,
    description,
    track,
    static,
    driver,
    event_type
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
