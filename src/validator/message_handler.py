from shared.message_queue import MessageQueue
import time
from validator.validator import Validator
from shared import logger


class MessageHandler:
    """ While there are messages from the shared messagequeue route the messages to the correct functions

    Read messages from the message queue and pass them to the validator.

    """
    def __init__(self):
        self.__validator = Validator()

    def process_messages(self):
        while True:
            msg = MessageQueue.get_message()
            if msg is None:
                logger.debug("No more messages, thread ends")
                return

            self.__validator.validate(msg)

            # time.sleep(0.1)  # for debugging
