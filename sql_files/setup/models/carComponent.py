class CarComponent:
    def __init__(self, id, semantic_type, manufacturer, serial_number, parent_component):
        self.id = id
        self.semantic_type = semantic_type
        self.manufacturer = manufacturer
        self.serial_number = serial_number
        self.parent_component = parent_component

    def to_dict(self):
        return {
            "id": self.id,
            "semanticType": self.semantic_type,
            "manufacturer": self.manufacturer,
            "serialNumber": self.serial_number,
            "parentComponent": self.parent_component
        }
