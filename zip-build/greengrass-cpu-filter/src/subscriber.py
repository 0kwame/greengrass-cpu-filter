import json
import logging
import awsiot.greengrasscoreipc  # use the newer IPC client
from awsiot.greengrasscoreipc.model import SubscribeToTopicRequest, QOS

logging.basicConfig(level=logging.INFO)

# class LocalSubscriber:
#     def __init__(self, topic: str, publisher):
#         self.logger = logging.getLogger("LocalSubscriber")
#         self.logger.info("Connecting to Greengrass Core")
#         self.client = awsiot.greengrasscoreipc.connect()  # <-- newer approach
#         self.logger.info("Connection complete!")
#         self.topic = topic
#         self.publisher = publisher
#         self.last_cpu_per_device = {}

#     def callback(self, event):
#         """Callback invoked for every message from the IoT device."""
#         payload = json.loads(event.payload.decode())
#         device_name = payload.get("device_name")
#         cpu = payload.get("cpu")

#         if device_name is None or cpu is None:
#             self.logger.warning(f"Invalid payload: {payload}")
#             return

#         last_cpu = self.last_cpu_per_device.get(device_name)

#         if last_cpu != cpu:
#             self.logger.info(f"CPU changed for {device_name}: {last_cpu} -> {cpu}")
#             self.publisher.send_message(payload)
#             self.last_cpu_per_device[device_name] = cpu
#         else:
#             self.logger.debug(f"No CPU change for {device_name}, ignoring")

#     def subscribe(self):
#         request = SubscribeToTopicRequest(topic=self.topic, qos=QOS.AT_LEAST_ONCE)
#         self.client.subscribe_to_topic(request, self.callback)

class LocalSubscriber:
    def __init__(self, topic: str):
        self.logger = logging.getLogger("LocalSubscriber")
        self.logger.info("Connecting to Greengrass Core")
        self.client = awsiot.greengrasscoreipc.connect()  # <-- newer approach
        self.logger.info("Connection complete!")
        self.topic = topic
        self.last_cpu_per_device = {}

    def callback(self, event):
        """Callback invoked for every message from the IoT device."""
        payload = json.loads(event.payload.decode())
        device_name = payload.get("device_name")
        cpu = payload.get("cpu")

        if device_name is None or cpu is None:
            self.logger.warning(f"Invalid payload: {payload}")
            return

        last_cpu = self.last_cpu_per_device.get(device_name)

        if last_cpu != cpu:
            self.logger.info(f"CPU changed for {device_name}: {last_cpu} -> {cpu}")
            # self.publisher.send_message(payload)
            self.last_cpu_per_device[device_name] = cpu
        else:
            self.logger.debug(f"No CPU change for {device_name}, ignoring")

    def subscribe(self):
        request = SubscribeToTopicRequest(topic=self.topic, qos=QOS.AT_LEAST_ONCE)
        self.client.subscribe_to_topic(request, self.callback)
