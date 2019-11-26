from confluent_kafka import Producer
import json
from bus_communication import load_credentials
from shared import logger


class BusProducer:
    def __init__(self):

        # Pre-shared credentials
        # self.credentials = json.load(open('bus_credentials.json'))

        self.credentials = load_credentials.LoadCredentials.load_bus_credentials()

        # Construct required configuration
        self.configuration = {
            'client.id': 'KB_producer',
            'group.id': 'KB_producer_group',
            'bootstrap.servers': ','.join(self.credentials['kafka_brokers_sasl']),
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': '/etc/ssl/certs',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.credentials['api_key'][0:16],
            'sasl.password': self.credentials['api_key'][16:48],
            'api.version.request': True
        }

        self.producer = Producer(self.configuration)

    def send(self, topic, message):

        logger.info("Sending: " + str(topic))
        logger.debug("Sending: " + str(topic) + ": " + str(message))
        # return

        # Produce and flush message to bus
        try:
            self.producer.produce(topic, message.encode('utf-8'), 'key', -1, self.on_delivery)
            self.producer.flush()
        except Exception as err:
            print('Sending data failed')
            print(err)
            return False

        return True

    def on_delivery(self, err, msg):
        if err:
            logger.error(str(err) + str(msg) + str(" (message wasn't successfully delivered to bus)"))

def main():
    with open('../prefab_TOPs/TOP803.json', 'r') as f:
        top803 = json.load(f)

    print(json.dumps(top803, indent=2))
    BusProducer().send(topic=top803['header']['topicName'], message=json.dumps(top803))
