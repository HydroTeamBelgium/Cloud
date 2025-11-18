INSERT IGNORE INTO drivers (
    id,
    name,
    dob,
    weight,
    length,
    sex,
    role
)
VALUES (%s, %s, %s, %s, %s, %s, %s);
