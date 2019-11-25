import shared
from shared import logger, message_queue
import json
import datetime
import random
import time
from bus_communication.bus_producer import BusProducer
import uuid
import threading


class Validator:
    bus_prod = BusProducer()

    @staticmethod
    def validate(message):
        """ read the message and determine if it is TOP 030 or TOP 803 and invoke the corresponding method
        """
        print("Thread id in validator: " + str(threading.get_ident()))
        try:
            inc_topic = message['header']['topicName']
        except (KeyError, TypeError, ValueError, IndexError) as e:
            logger.warning("could not read topicName from message. Do nothing")
            logger.debug(e)
            logger.debug(message)
            return

        logger.info("Message is now processed. TOPIC: " + str(inc_topic))

        if inc_topic == 'TOP030_REPORT_REQUESTED':
            Validator.validate_TOP030(message)
        elif inc_topic == 'TOP803_WEATHER_REPORT':
            Validator.validate_803(message)
        else:
            logger.warning("Message read in validator is not TOP030 nor TOP803")

    @staticmethod
    def validate_TOP030(message):
        """
        δες αν εχει ξαναέρθει msg με το incident id που ηρθε τώρα (είτε τοπικά είτε σε ένα csv).
            Αν έχει ξαναέρθει, τότε μην κάνεις τίποτα.
            Αν δεν έχει ξαναέρθει, τότε δες αν το msg έχει attachment και incident type.
                Αν δεν έχει, τότε μην κάνεις τίποτα
                Αν έχει (incident type)
                    Βαλε το στην λίστα των incidents που έχουν επεξεργαστεί (έχουν περάσει ή περνανε τώρα validation)
                    Δες αν το incident type == Heatwave (?)
                        Αν οχι, τότε βάλε spam == false
                        Αν ναι, τότε ρώτα το CRCL για να σου πει τις καιρικές συνθήκες στην περιοχή του incident
        """

        report_id = None
        report_type = None
        inc_long = None
        inc_lat = None
        report_time = None
        report_spam = None

        # print(message['body']['incidents'])
        logger.debug("Processed TOP030 message: " + str(message))

        header = message['header']

        try:
            inc_long = float(message['body']['position']['long'])
            inc_lat = float(message['body']['position']['lat'])
        except (KeyError, TypeError, ValueError, IndexError) as e:
            logger.info("Incoming message does not have location, validation will stop.")
            logger.debug(str(type(e)) + str(e))
            logger.debug(message)
            return

        try:
            incidents = message['body']['incidents']
        except (KeyError, TypeError) as e:
            logger.info("No reports in TOP030, validation will stop.")
            logger.debug(str(type(e)) + str(e))
            logger.debug(str(message))
            return

        if len(incidents) == 0:
            logger.info("No incidents in TOP030.")

        for inc in incidents:
            try:
                report_id = inc['reportId']
                report_type = inc['incidentType']
                report_time = inc['timestamp']
            except (KeyError, TypeError, ValueError, IndexError) as e:
                logger.warning("Incident does not have report ID / incident Type / timestamp")
                logger.debug(str(type(e)) + str(e))
                return

            if report_id in shared.processed_mgs:
                logger.debug("Report already processed. ReportId: " + str(report_id))
                continue

            shared.processed_mgs[report_id] = {'inc': inc, 'header': header}

            # TODO: check if spam field is already there, and if is spam=True/False stop validation (not None)

            logger.info("Report is checked to determine if it is spam. ID:" + str(report_id))
            if Validator.__rule_1_pre(report_type) is True:
                logger.info("Asking CRCL for report with ID:" + str(report_id))
                t_802 = Validator.generate_TOP802(message, report_id, inc_long, inc_lat, report_time)
                Validator.bus_prod.send(topic=t_802['header']['topicName'], message=json.dumps(t_802))
            else:
                Validator.__incident_spam(report_id, False)

    @staticmethod
    def __rule_1_pre(inc_type):
        """ check if the incident requires more investigation (request more info from CRCL) """

        if inc_type == 'Precipitation' or inc_type == 'HeavyPrecipitation' or inc_type == 'Heavy Precipitation' or inc_type == "Blizzard":
            is_suspicious = True
        else:
            is_suspicious = False
        return is_suspicious

    @staticmethod
    def __rule_1_after(msg):
        """ after the reply from CRCL is back check if the incident is spam """

        try:
            report_id = msg['body']['reportID']
            report_type = shared.processed_mgs[report_id]['inc']['incidentType']
            precipitation = msg['body']['precipitation']
        except (KeyError, TypeError, ValueError, IndexError) as e:
            logger.info("Cannot load reportID / report type/ precipitation from processed messages")
            logger.debug(str(type(e)) + str(e))
            return

        logger.info("Validating 803 with type: " + str(report_type) + " and precipitation: " + str(
            round(precipitation, 2)) + " ID: " + str(report_id))

        # type == Precipitation / Heavy Precipitation + precipitation == 0 ==> SPAM
        if precipitation < .1 and (
                report_type == 'Precipitation' or report_type == 'HeavyPrecipitation' or
                report_type == 'Heavy Precipitation' or report_type == "Blizzard"):
            spam = True
        else:
            spam = False
        return spam

    @staticmethod
    def validate_803(msg):
        """ find the incident in the local storage and continue validation according to the info from TOP803"""

        logger.info("Message TOP803 is processed.")
        logger.debug("TOP803 message: " + str(msg))

        if msg['body']['reportID'] not in shared.processed_mgs:
            logger.warning(
                "Message TOP803 does not correspond to a stored report. ID: " + str(msg['body']['reportID']))
            return

        Validator.__incident_spam(msg['body']['reportID'], Validator.__rule_1_after(msg))

    @staticmethod
    def __incident_spam(reportID, spam):
        """ when the message is detected to be spam send update TOP801 to KBS

        Even if not detected to be spam inform KBS via TOP801. So that it is known that the validation step works.
        """
        logger.info("Message IS " + ('' if spam else 'NOT ') + "SPAM! " + " Passed validation successfully. ID: " + str(
            reportID))
        msg = Validator.generate_TOP801(reportID, spam)

        if msg is None:
            logger.warning("TOP801 was not generated correctly")
            return

        Validator.bus_prod.send(topic=msg['header']['topicName'], message=json.dumps(msg))

    @staticmethod
    def generate_header(header, topic_name, action_type=None):
        """ generate a dict corresponding to header with correct values and return it

        Read the immutable values from the parameter header
        Generate the values that are specific
        Return the header dict
        If something goes wrong, then return None
        """
        try:
            header['topicName'] = topic_name
            if action_type is not None:
                header['actionType'] = action_type
            header['sentUTC'] = Validator.time_UTC()
            header['sender'] = 'VAL'
            header['msgIdentifier'] = 'VAL_' + str(uuid.uuid4()) + "_" + str(time.time())
        except(KeyError, TypeError, ValueError, IndexError) as e:
            logger.error(
                "validator.Validator.generate_header: header does not have the correct fields")
            logger.debug(str(type(e)) + str(e))
            return None

        return header

    @staticmethod
    def generate_TOP801(reportID, spam):

        try:
            header = shared.processed_mgs[reportID]['header']
        except(KeyError, TypeError, ValueError, IndexError) as e:
            logger.error("header cannot be found in processed messages")
            logger.debug(str(type(e)) + str(e))
            return None

        msg = dict()
        msg['header'] = Validator.generate_header(header, topic_name=shared.TOP801)
        msg['body'] = dict()
        msg['body']['incidentID'] = reportID
        msg['body']['spam'] = spam

        return msg

    @staticmethod
    def time_UTC():
        return datetime.datetime.utcnow().isoformat().split(".")[0] + 'Z'

    @staticmethod
    def generate_TOP802(message, report_id, inc_long, inc_lat, report_time):
        """ ask CRCL for data about the relvant incident """

        msg = dict()
        msg['header'] = Validator.generate_header(message['header'], topic_name=shared.TOP802)
        msg['body'] = dict()
        msg['body']['reportID'] = report_id
        msg['body']['position'] = {"latitude": inc_lat, 'longitude': inc_long}
        msg['body']['reportTimeStampUTC'] = report_time

        # testing
        # time.sleep(random.random() * 1.0)
        # message_queue.MessageQueue.put_message(Validator.__generate_dummy_803(report_id))

        # t803 = Validator.__generate_dummy_803(report_id)
        # Validator.bus_prod.send(topic=shared.TOP803, message=json.dumps(t803))

        # from bus_communication import bus_consumer
        # bus_consumer.ValidatorThreadHandler.init_validator()

        return msg

    @staticmethod
    def __generate_dummy_803(inc_id):
        """ return a fake reply as it is expected from TOP803 with random values """

        msg = {
            "header": {
                "topicName": "TOP803_WEATHER_REPORT",
                "topicMajorVersion": 0,
                "topicMinorVersion": 3,
                "sender": "CRCL",
                "msgIdentifier": 542853,
                "sentUTC": "2018-01-01T12:00:00Z",
                "status": "Actual",
                "actionType": "Update",
                "scope": "Restricted",
                "district": "Thessaloniki",
                "code": 20190617001
            },
            "body": {
                "reportID": inc_id,
                "reportTimeStampUTC": "2018-01-01T11:50:00Z",
                "temperature": random.random() * 20 + 20,
                "humidity": random.random() * 100,
                "wind": random.random() * 15,
                "precipitation": random.random() * 2 if random.random() > 0.7 else 0
            }
        }
        # print("'Send 803':" + json.dumps(msg))
        return msg


if __name__ == '__main__':
    print(Validator.time_UTC())
    msg = """{
    "header": {
        "topicName": "TOP801_INCIDENT_VALIDATION",
        "topicMajorVersion": 0,
        "topicMinorVersion": 3,
        "sender": "VAL",
        "msgIdentifier": 542853,
        "sentUTC": "2018-01-01T12:00:00Z",
        "status": "Actual",
        "actionType": "Update",
        "scope": "Restricted",
        "district": "Thessaloniki",
        "code": 20190617001
    },
    "body": {
        "reportID": 542853,
        "spam": false
    }
}"""
    print(json.dumps(Validator.generate_header(json.loads(msg)['header'], topic_name='TOP801_dummy'), indent=2))
