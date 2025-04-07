class SensorData:
    def __init__(self, sensor_id, value, timestamp, event_id):
        self.sensor_id = sensor_id
        self.value = value
        self.timestamp = timestamp
        self.event_id = event_id

    def to_dict(self):
        return {
            "sensor_id": self.sensor_id,
            "value": self.value,
            "timestamp": self.timestamp,
            "event_id": self.event_id
        }
