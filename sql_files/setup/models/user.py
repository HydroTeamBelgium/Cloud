class User:
    """
    Represents a user in the system.
    Attributes:
        id (int): Unique identifier for the user.
        username (str): Username of the user.
        email (str): Email address of the user.
        admin (bool): Indicates if the user is an admin.
        password (str): Password of the user.
        active_session (bool): Indicates if the user has an active session.
    """
    def __init__(self, id, username, email, admin, password, active_session):
        self.id = id
        self.username = username
        self.email = email
        self.admin = admin
        self.password = password
        self.active_session = active_session

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
            "admin": self.admin,
            "password": self.password,
            "activeSession": self.active_session
        }
