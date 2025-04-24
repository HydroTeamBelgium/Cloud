INSERT IGNORE INTO drivers (
    given_name,
    code,
    date_of_birth
)
VALUES (%s, %s, %s);
