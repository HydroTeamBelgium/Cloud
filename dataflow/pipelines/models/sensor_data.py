class SensorData:
    def __init__(self, sensor_id, timestamp, value, event_id):
        """Initializes a SensorData object.

        Args:
            sensor_id: str, unique identifier for the sensor.
            timestamp: str, timestamp of the reading.
            value: float, sensor reading value.
            event_id: int, ID of the associated event from the events table.
        """
        self.sensor_id = sensor_id
        self.timestamp = timestamp
        self.value = value
        self.event_id = event_id