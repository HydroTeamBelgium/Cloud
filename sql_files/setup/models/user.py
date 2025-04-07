class User:
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
