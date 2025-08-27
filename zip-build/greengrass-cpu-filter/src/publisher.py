import json
import logging
import awsiot.greengrasscoreipc
import awsiot.greengrasscoreipc.model as model

TOPIC = "iot/metrics/cpu"

logging.basicConfig(level=logging.INFO)

class IoTPublisher:
    def __init__(self):
        self.logger = logging.getLogger("IoTPublisher")
        self.logger.info("Connecting to Greengrass Core")
        self.ipc_client = awsiot.greengrasscoreipc.connect()
        self.logger.info("Connection complete!")

    def send_message(self, message: str):
        payload = {"greeting": "Hello", "subject": message}
        op = self.ipc_client.new_publish_to_iot_core()
        op.activate(model.PublishToIoTCoreRequest(
            topic_name=TOPIC,
            qos=model.QOS.AT_LEAST_ONCE,
            payload=json.dumps(payload).encode(),
        ))

        try:
            result = op.get_response().result(timeout=5.0)
            self.logger.info("successfully published message:", result)
        except Exception as e:
            self.logger.error("failed to publish message:", e)

# class IoTPublisher:
#     def __init__(self):
#         self.client = awsiot.greengrasscoreipc.connect()
#         self.cloud_topic = "cloud/metrics/cpu"

#     def send_message(self, message: dict):
#         try:
#             request = PublishToTopicRequest()
#             request.topic = self.cloud_topic
#             request.payload = json.dumps(message).encode()
#             self.client.publish_to_topic(request)
#             logging.info(f"Published to cloud: {message}")
#         except Exception as e:
#             logging.error(f"Error publishing: {e}")
