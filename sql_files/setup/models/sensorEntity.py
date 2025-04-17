from dataclasses import dataclass

@dataclass
class SensorEntity:
    """
    Represents a sensor entity with its attributes.
    Attributes:
        id (int): Unique identifier for the sensor entity.
        serialNumber (str): Serial number of the sensor.
        purchaseDate (str): Purchase date of the sensor.
        sensorType (int): Type of the sensor.
        readingEndPoint (int): Reading endpoint associated with the sensor.
        sensor_table (str): Name of the table associated with the sensor.
    """
    id: int
    serialNumber: str
    purchaseDate: str
    sensorType: int
    readingEndPoint: int
    sensor_table: str

    def to_dict(self):
        return {
            "id": self.id,
            "serialNumber": self.serialNumber,
            "purchaseDate": self.purchaseDate,
            "sensorType": self.sensorType,
            "readingEndPoint": self.readingEndPoint,
            "sensor_table": self.sensor_table,
        }
