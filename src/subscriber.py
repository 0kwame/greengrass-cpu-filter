import json
from typing import cast
import logging
import awsiot.greengrasscoreipc
import awsiot.greengrasscoreipc.model as model
from awsiot.greengrasscoreipc.client import SubscribeToTopicStreamHandler
from awsiot.greengrasscoreipc.model import BinaryMessage

logging.basicConfig(level=logging.INFO)

class MyStreamHandler(SubscribeToTopicStreamHandler):
    def __init__(self, subscriber):
        super().__init__()
        self.subscriber = subscriber

    def on_stream_event(self, event):
        try:
            print(f"message receving...", event)
            if hasattr(event, "json_message") and event.json_message:
                message = event.json_message.message
                if message:
                    self.subscriber.handle_message(message)
            elif hasattr(event, "binary_message") and isinstance(event.binary_message, BinaryMessage):
                payload_bytes = event.binary_message.message
                if payload_bytes:
                    message = json.loads(payload_bytes.decode())
                    self.subscriber.handle_message(message)
            else:
                self.subscriber.logger.debug(f"Skipping unsupported message: {event}")
        except Exception as e:
            self.subscriber.logger.error(f"Error handling event: {e}", exc_info=True)


    def on_stream_error(self, error) -> bool:
        self.subscriber.logger.error(f"Stream error: {error}")
        return False  # stop on error

    def on_stream_closed(self):
        self.subscriber.logger.info("Stream closed")


class LocalSubscriber:
    def __init__(self, topic: str):
        self.logger = logging.getLogger("LocalSubscriber")
        self.logger.info("Connecting to Greengrass Core IPC")
        self.client = awsiot.greengrasscoreipc.connect()
        self.logger.info("Connection complete!")
        self.topic = topic
        self.last_cpu_per_device = {}

    def handle_message(self, message: dict):
        """
        Process messages that match the expected structure:
        {'device_name': str, 'cpu': float, 'timestamp': str}
        """
        # Validate message structure
        if not isinstance(message, dict):
            self.logger.warning(f"Message is not a dictionary: {type(message)}")
            return
            
        if not all(k in message for k in ("device_name", "cpu", "timestamp")):
            self.logger.debug(f"Message missing required fields: {message}")
            return

        try:
            device_name = str(message["device_name"])
            cpu = float(message["cpu"])
            timestamp = str(message['timestamp'])
        except (ValueError, TypeError) as e:
            self.logger.error(f"Invalid message format: {e}")
            return

        last_cpu = self.last_cpu_per_device.get(device_name)
        self.logger.info(f"Received message at timestamp: {timestamp}")

        # Check if CPU value has changed (with small tolerance for floating point comparison)
        if last_cpu is None or abs(last_cpu - cpu) > 0.01:
            self.logger.info(f"CPU changed for {device_name}: {last_cpu} -> {cpu}")
            self.last_cpu_per_device[device_name] = cpu
            # Publish to IoT Core only if changed
            self.publish_to_iot_core(message)
        else:
            self.logger.debug(f"No significant CPU change for {device_name}, ignoring")

    def publish_to_iot_core(self, message: dict):
        """Forward filtered message to IoT Core"""
        try:
            request = model.PublishToIoTCoreRequest(
                topic_name="iot/metrics/cpu",   # IoT Core topic
                qos=model.QOS.AT_LEAST_ONCE,
                payload=json.dumps(message).encode()
            )
            op = self.client.new_publish_to_iot_core()
            future = op.activate(request)
          # Reduced timeout and better error handling
            try:
                result = future.result(timeout=5.0)  # Reduced from 10 to 5 seconds
                self.logger.info(f"Successfully forwarded message to IoT Core: {message['device_name']}")
            except TimeoutError:
                self.logger.error(f"Timeout publishing to IoT Core for device: {message['device_name']}")
            except Exception as publish_error:
                self.logger.error(f"Publish operation failed: {publish_error}")
        except Exception as e:
                self.logger.error(f"Failed to create publish request: {e}", exc_info=True)

    def subscribe(self):
        """Subscribe to local pub/sub topic"""
        self.logger.info(f"Subscribing to local topic: {self.topic}")
        handler = MyStreamHandler(self)
        request = model.SubscribeToTopicRequest(
            topic=self.topic,
            receive_mode=model.ReceiveMode.RECEIVE_ALL_MESSAGES
        )

        operation = self.client.new_subscribe_to_topic(handler)
        future = operation.activate(request)

        try:
            future.result(timeout=5)
            self.logger.info(f"Successfully subscribed to {self.topic}")
            return operation  # Return operation to keep subscription alive
        except Exception as e:
            self.logger.error(f"Subscription failed: {e}")
            raise

    def cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'client'):
                self.client.close()
                self.logger.info("IPC client closed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
