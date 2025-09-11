from typing import TypedDict


class DeviceMessage(TypedDict):
    device_name: str
    cpu: float
    timestamp: str  # ISO8601 string
