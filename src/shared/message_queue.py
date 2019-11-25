import queue
import threading
import time
from shared import logger


class MessageQueue:
    __queue = queue.Queue()

    @staticmethod
    def get_message():
        try:
            return MessageQueue.__queue.get(block=True, timeout=5)
            # Note that the command blocks the execution of the threat (block=True)
            # so the listener should be on a different thread (and have a chance to add something in there)
        except queue.Empty:
            logger.debug("message_queue is empty!")
            return None

    @staticmethod
    def put_message(msg):
        try:
            MessageQueue.__queue.put(item=msg, block=True, timeout=5)
            # logger.debug("Message put in queue: " + str(msg))
            return True
        except queue.Full as e:
            logger.debug("message_queue error. Could not put message in queue. (Full)")
            logger.debug(e)
            return False


if __name__ == "__main__":
    print("hello ")


    def eat():
        print("Eater")
        time.sleep(0.1)
        for j in range(1000):
            print("Consumed " + str(MessageQueue.get_message()))


    counter = 1

    thread_1 = threading.Thread(target=eat)
    thread_1.start()

    # time.sleep(0.2)
    for i in range(1000):
        MessageQueue.put_message("Test" + str(counter))
        print("Created: Test" + str(counter))
        counter += 1

    # thread_1.join()
