class SensorEntity:
    def __init__(self, id, serial_number, purchase_date, sensor_type, reading_end_point, sensor_table):
        self.id = id
        self.serial_number = serial_number
        self.purchase_date = purchase_date
        self.sensor_type = sensor_type
        self.reading_end_point = reading_end_point
        self.sensor_table = sensor_table

    def to_dict(self):
        return {
            "id": self.id,
            "serialNumber": self.serial_number,
            "purchaseDate": self.purchase_date,
            "sensorType": self.sensor_type,
            "readingEndPoint": self.reading_end_point,
            "sensor_table": self.sensor_table
        }
