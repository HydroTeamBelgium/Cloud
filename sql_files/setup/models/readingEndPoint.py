class ReadingEndPoint:
    def __init__(self, id, name, functional_group, car_component):
        self.id = id
        self.name = name
        self.functional_group = functional_group
        self.car_component = car_component

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "functionalGroup": self.functional_group,
            "carComponent": self.car_component
        }
