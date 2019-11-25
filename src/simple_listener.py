from bus_communication.bus_consumer import BusConsumer
import shared
import json
import datetime


def custom_print(msg):
    print("")
    print(str(datetime.datetime.utcnow().isoformat().split(".")[0] + 'Z') + ": " + str(
        json.loads(msg)['header']['topicName']) + " : " + str(msg))


def main():
    bp = BusConsumer(groupid="simple_listener")
    bp.listen(performed_action=custom_print,
              topics=[shared.TOP801, shared.TOP802, shared.TOP803, shared.TOP030, shared.TOP001, shared.TOP028,
                      shared.TOP021, shared.TOP101, "TOP019_UAV_media_analyzed", "TOP805_KBS_TRIGGERS"])


if __name__ == '__main__':
    main()
