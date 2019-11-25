from confluent_kafka import Consumer
from shared import load_credentials, TOP030, TOP803
import time
from shared import message_queue, logger
from validator import message_handler
import threading
import json


def message_to_queue(message):
    try:
        message = json.loads(message)
    except json.decoder.JSONDecodeError as e:
        logger.warning("message from bus is not a valid json: " + str(e))
        logger.debug(message)
        return
    message_queue.MessageQueue.put_message(message)
    logger.info("Message arrived from bus and inserted in queue")
    logger.debug(json.dumps(message))
    ValidatorThreadHandler.init_validator()


class ValidatorThreadHandler:
    """ Handles the validator thread

    If validator thread runs then do nothing, if not, then start one thread.

    """
    __process_method = message_handler.MessageHandler().process_messages
    __validator_thread = threading.Thread(target=__process_method)

    @staticmethod
    def init_validator():
        """ check if validating thread is alive and if not, then initiate it """
        if not ValidatorThreadHandler.__validator_thread.is_alive():
            print(threading.get_ident())
            logger.info("Creating validator thread")
            print('Creating validator thread')
            ValidatorThreadHandler.__validator_thread = threading.Thread(target=ValidatorThreadHandler.__process_method)
            ValidatorThreadHandler.__validator_thread.start()
            print(ValidatorThreadHandler.__validator_thread)

    @staticmethod
    def join_validation_thread():
        ValidatorThreadHandler.__validator_thread.join()


class BusConsumer:
    def __init__(self, groupid=None):

        # Pre-shared credentials
        # self.credentials = json.load(open('bus_credentials.json'))

        self.credentials = load_credentials.LoadCredentials.load_bus_credentials()

        # Construct required configuration
        self.configuration = {
            'client.id': 'VAL_consumer',
            'group.id': 'VAL_consumer_group',
            'bootstrap.servers': ','.join(self.credentials['kafka_brokers_sasl']),
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': '/etc/ssl/certs',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.credentials['api_key'][0:16],
            'sasl.password': self.credentials['api_key'][16:48],
            'api.version.request': True
        }

        if groupid is not None:
            self.configuration["group.id"] = groupid

        self.consumer = Consumer(self.configuration)

        self.listening = True

        self.database = 'messages.sqlite'

        self.default_topics = [TOP803, TOP030]

    def listen(self, performed_action, topics=None):
        # Topics should be a list of topic names e.g. ['topic1', 'topic2']
        if topics is None:
            topics = self.default_topics

        self.listening = True

        # Subscribe to topics
        try:
            self.consumer.subscribe(topics)
        except Exception as e:
            logger.error("Error @ BusConsumer.listen()")
            logger.debug(str(type(e)) + str(e))
            return False
        logger.info("listener subscribed successfully to topics:" + str(topics))

        # Initiate a loop for continuous listening
        while self.listening:
            msg = self.consumer.poll(0)

            # If a message is received and it is not an error message
            if msg is not None and msg.error() is None:

                # Add incoming message to requests database
                try:
                    message_text = msg.value().decode('utf-8')
                except:
                    message_text = msg.value()

                performed_action(message_text)

            # TODO: check if it works ok with the sleep .5 
            time.sleep(0.5)

        # Unsubscribe and close consumer
        self.consumer.unsubscribe()
        self.consumer.close()

    def stop(self):
        self.listening = False

    @staticmethod
    def __load_dummy_messages():
        """ Load Vicenza messages and add them to the message queue with a small delay between each insertion """
        import random
        import filter_messages

        max_delay = 0.01  # delay in the range [0, max_delay] from uniform distribution

        vic_messages = filter_messages.simulateData()
        for m in vic_messages:
            logger.debug("writing TOP101 message to queue")
            message_queue.MessageQueue.put_message(m)  # Note: pass it by value, not reference!
            ValidatorThreadHandler.init_validator()
            time.sleep(random.random() * max_delay)

        ValidatorThreadHandler.join_validation_thread()

    @staticmethod
    def __continuously_add_fake_TOP101():
        import random
        fake_msg = dict()
        fake_msg['body'] = dict()
        fake_msg['body']['spam'] = False
        fake_msg['body']['incidentID'] = random.randint(0, 1000000)
        p = 1
        while True:
            if random.random() > 1:
                p += 1
                fake_msg['body']['incidentID'] = p  # random.randint(0, 1000000)
                print("message in queue. ID: ", p)
                message_queue.MessageQueue.put_message(
                    {'body': {'spam': False, 'incidentID': p}})
            ValidatorThreadHandler.init_validator()

    @staticmethod
    def __load_TOP030():
        """ load TOP030 messages from local file and put them in the message queue """
        import random
        import filter_messages

        max_delay = .1  # delay in the range [0, max_delay] from uniform distribution

        vic_messages = filter_messages.get030()
        for m in vic_messages:
            logger.debug("writing TOP030 message to queue")
            message_queue.MessageQueue.put_message(m)  # Note: pass it by value, not reference!
            ValidatorThreadHandler.init_validator()
            time.sleep(random.random() * max_delay)

        ValidatorThreadHandler.join_validation_thread()


def main(start=0, end=None):
    import random
    from datetime import datetime
    from bus_communication import bus_producer

    with open("VAL_TOP030.json", 'r') as f:
        top030 = json.load(f)

    bp = bus_producer.BusProducer()

    max_delay = 1  # delay in the range [0, max_delay] from uniform distribution

    if end is None: end = len(top030)

    count = 0
    for m in top030:
        if count >= end:
            break
        count += 1
        if count < start:
            continue

        logger.info("sending message 30 to bus : " + str(count))

        try:
            m['header']['sentUTC'] = datetime.utcnow().isoformat().split(".")[0] + 'Z'
        except:
            pass

        try:
            if 'incidents' in m['body']:
                for inc in m['body']['incidents']:
                    inc['timestamp'] = datetime.utcnow().isoformat().split(".")[0] + 'Z'
        except:
            pass

        # print(json.dumps(m, indent=2))
        bp.send(topic=m['header']['topicName'], message=json.dumps(m))

        time.sleep(random.random() * max_delay)


if __name__ == '__main__':
    main()
    exit()

    main(start=0, end=2)
    main(start=2, end=4)

