import threading
from datetime import datetime
import json
import logging
import sys
import time
import psutil
from publisher import IoTPublisher
from subscriber import LocalSubscriber

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

def generate_cpu_message(device_name="gg-core-demo-1"):
    cpu_value = psutil.cpu_percent(interval=1)  # gets actual CPU usage %
    timestamp = datetime.now().isoformat() + "Z"
    message = {
        "device_name": device_name,
        "cpu": cpu_value,
        "timestamp": timestamp
    }
    return message

def run_publisher():
    logging.info("Publisher started")
    publisher = IoTPublisher()
    while True:
        msg = generate_cpu_message()
        logging.info(f"Publishing: {msg}")
        publisher.send_message(msg)
        time.sleep(2)

def run_subscriber():
    logging.info("Subscriber started")
    sub = LocalSubscriber("iot/metrics/cpu")
    sub.subscribe()  # non-blocking subscription
    # Keep the subscriber thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Subscriber shutting down...")

if __name__ == "__main__":
    # Start publisher in a separate thread
    publisher_thread = threading.Thread(target=run_publisher, daemon=True) 
    publisher_thread.start()
    
    # Start subscriber in main thread (or another thread if preferred)
    run_subscriber()