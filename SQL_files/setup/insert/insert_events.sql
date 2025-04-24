INSERT IGNORE INTO events (
    id,
    name,
    startDate,
    endDate,
    location
)
VALUES (%s, %s, %s, %s, %s);
