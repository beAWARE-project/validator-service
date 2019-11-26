from bus_communication import bus_consumer


def main():
    consumer = bus_consumer.BusConsumer()
    topics = None
    """topics = [
        TOP803,
        TOP101,
        'TOP019_UAV_media_analyzed',
        'TOP112_SUMMARY_TRIGGER',
        'TOP106_RISK_MAPS', 'TOP017_video_analyzed',
        'TOP018_image_analyzed',
        'TOP010_AUDIO_ANALYZED',
        'TOP030_REPORT_REQUESTED',
        'TOP040_TEXT_REPORT_GENERATED',
        'TOP007_UPDATE_INCIDENT_RISK',
        'TOP104_METRIC_REPORT',
        'TOP022_PUBLIC_ALERT',
        'TOP102_TEAM_REPORT',
        'TOP023_TASK_ASSIGNMENT',
        'TOP103_TASK_REPORT',
        'TOP006_INCIDENT_REPORT_CRCL',
        'TOP021_INCIDENT_REPORT',
        'TOP001_SOCIAL_MEDIA_TEXT',
        'TOP003_SOCIAL_MEDIA_REPORT',
        'TOP031_UAVP_MESSAGE'
    ]"""
    consumer.listen(performed_action=bus_consumer.message_to_queue, topics=topics)


if __name__ == "__main__":
    main()
