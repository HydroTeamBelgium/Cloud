from dataclasses import dataclass

@dataclass
class SensorEntity:
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
