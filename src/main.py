import time
from publisher import IoTPublisher
from subscriber import LocalSubscriber

def main():
    publisher = IoTPublisher()
    subscriber = LocalSubscriber("iot/metrics/cpu", publisher)
    subscriber.subscribe()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
