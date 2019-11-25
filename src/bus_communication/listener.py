"""
OBSOLETE, see bus_consumer instead
"""

import time
from shared import message_queue, logger
from validator import message_handler
import threading


def consume():
    # TODO:
    #  read form bus
    #  if messages to be read
    #       for each message
    #           put it to message queue
    #           probe __validator_thread to start
    pass


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
            logger.info("Creating validator thread")
            print('Creating validator thread')
            ValidatorThreadHandler.__validator_thread = threading.Thread(target=ValidatorThreadHandler.__process_method)
            ValidatorThreadHandler.__validator_thread.start()


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


def listener():
    """ Periodically read messages from the bus and if there are, then add them to the queue and probe the validation"""
    __load_dummy_messages()
    # __continuously_add_fake_TOP101()

    while True:
        consume()  # DUMMY
        time.sleep(.42)
