INSERT IGNORE INTO users (
    id,
    username,
    email,
    authorisation,
    password,
    active_session
)
VALUES (%s, %s, %s, %s, %s, %s);
