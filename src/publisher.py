import json
import logging
import awsiot.greengrasscoreipc
import awsiot.greengrasscoreipc.model as model

TOPIC = "iot/metrics/cpu"   # local pub/sub topic

logging.basicConfig(level=logging.INFO)

class IoTPublisher:
    def __init__(self):
        self.logger = logging.getLogger("IoTPublisher")
        self.logger.info("Connecting to Greengrass Core IPC")
        self.ipc_client = awsiot.greengrasscoreipc.connect()
        self.logger.info("Connection complete!")

    def send_message(self, message: dict):
        try:
            request = model.PublishToTopicRequest()
            request.topic = TOPIC
            request.publish_message = model.PublishMessage(
                json_message=model.JsonMessage(
                    message=json.loads(json.dumps(message))  # ensures valid JSON
                )
            )

            operation = self.ipc_client.new_publish_to_topic()
            operation.activate(request)
            operation.get_response().result(timeout=10.0)

            self.logger.info(f"Successfully published locally: {message}")
        except Exception as e:
            self.logger.error(f"Failed to publish locally: {e}")
