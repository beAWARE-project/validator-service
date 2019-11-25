import logging


class ProcessedMessages:
    """ stores the messages that have been processed, either on RAM or on a file (to ensure persistence) """

    # TODO: determine if we need this; implement class
    def __init__(self, ):
        self.__processed_msgs = dict()
        self.__local = False

    def get_incident(self, inc_id):
        return self.__processed_msgs[inc_id]

    def get_all_incidents(self):
        pass

    def add_incident(self, incident):
        pass

    def clear_incidents(self):
        pass


processed_mgs = dict()

TOP001 = 'TOP001_SOCIAL_MEDIA_TEXT'
TOP101 = 'TOP101_INCIDENT_REPORT'
TOP801 = 'TOP801_INCIDENT_VALIDATION'
TOP802 = 'TOP802_WEATHER_REQUEST'
TOP803 = 'TOP803_WEATHER_REPORT'
TOP030 = 'TOP030_REPORT_REQUESTED'
TOP028 = 'TOP028_TEXT_ANALYSED'
TOP021 = 'TOP021_INCIDENT_REPORT'

logger = logging.getLogger('Validator')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
__fh = logging.FileHandler('spam.log')
__fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
__ch = logging.StreamHandler()
__ch.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
__formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(message)s')
__fh.setFormatter(__formatter)
__ch.setFormatter(__formatter)

# add the handlers to the logger
logger.addHandler(__fh)
logger.addHandler(__ch)

