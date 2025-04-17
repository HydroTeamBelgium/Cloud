INSERT IGNORE INTO events (
    round,
    raceName,
    date,
    time
) VALUES (%s, %s, %s, %s);