"""Data models for the racing car telemetry system.

This package contains data model classes that represent the various
entities in the racing car telemetry system, such as sensor readings,
car components, and race events.
"""

from .sensor_data import SensorReading, CarComponent, RaceEvent

__all__ = [
    'SensorReading',
    'CarComponent',
    'RaceEvent'
] 