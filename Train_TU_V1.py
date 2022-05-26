from PyQt5.QtWidgets import QApplication, QLabel, QWidget, QGridLayout
from PyQt5.QtCore import QThread, QObject, pyqtSignal, pyqtSlot
from google.transit import gtfs_realtime_pb2
import urllib.request
import time
import sys
import csv
import datetime
import logging
__author__ = 'Jianqing Wu'

def main(): 
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)
    handler = logging.FileHandler('train-tu-log-v1')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    time_str = '01:30:00'
    now = datetime.datetime.now()
    schedule_time = datetime.datetime.strptime(time_str, '%H:%M:%S').replace(year=now.year, month=now.month,
                                                                             day=now.day)
    if schedule_time < now:
        schedule_time = schedule_time + datetime.timedelta(days=1)
    filename = datetime.datetime.now().strftime("%Y-%m-%d")
    feed = gtfs_realtime_pb2.FeedMessage()
    req = urllib.request.Request('https://api.transport.nsw.gov.au/v1/gtfs/realtime/sydneytrains')
    req.add_header('Authorization', 'apikey ')
    i = 0
    while True:
        response = urllib.request.urlopen(req)
        feed.ParseFromString(response.read())
        entity_id = []
        trip_id = []
        schedule_relationship = []
        route_id =[]
        timestamp =[]
        arrival_delay = []
        arrival_time = []
        departure_delay = []
        departure_time = []
        stop_id = []
        stop_schedule =[]
        for entity in feed.entity:
            if entity.HasField('trip_update'):

                if entity.trip_update.stop_time_update:
                    for stop_count in range(len(entity.trip_update.stop_time_update)):
                        entity_id.append(entity.id)
                        trip_id.append(entity.trip_update.trip.trip_id)
                        schedule_relationship.append(entity.trip_update.trip.schedule_relationship)
                        route_id.append(entity.trip_update.trip.route_id)
                        timestamp.append(entity.trip_update.timestamp)
                        arrival_delay.append(entity.trip_update.stop_time_update[stop_count].arrival.delay)
                        arrival_time.append(entity.trip_update.stop_time_update[stop_count].arrival.time)
                        departure_delay.append(entity.trip_update.stop_time_update[stop_count].departure.delay)
                        departure_time.append(entity.trip_update.stop_time_update[stop_count].departure.time)
                        stop_id.append(entity.trip_update.stop_time_update[stop_count].stop_id)
                        stop_schedule.append(entity.trip_update.stop_time_update[stop_count].schedule_relationship)

        time_before_start = int(round((schedule_time - datetime.datetime.now()).total_seconds()))

        if(time_before_start/10 < 0):
            filename = (datetime.datetime.now()).strftime("%Y-%m-%d")
            schedule_time = schedule_time + datetime.timedelta(days=1)
            now = datetime.datetime.now()
            print('New Schedule_time: ', schedule_time)
            print('New File_name: ', filename)
            i = 0
        with open('Train-TU-{}.csv'.format(filename), "a", encoding="UTF8", newline='') as csv_row:
            writer = csv.writer(csv_row)
            writer.writerows(
                zip(entity_id, trip_id, schedule_relationship, route_id, timestamp, arrival_delay, arrival_time,
                    departure_delay, departure_time, stop_id, stop_schedule))
            i += 1
        time.sleep(10)






if __name__ == "__main__":
    main()