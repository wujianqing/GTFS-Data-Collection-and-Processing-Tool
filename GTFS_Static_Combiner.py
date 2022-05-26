import pandas as pd
import datetime
import os
from os.path import join
import re
__author__ = 'Jianqing Wu'

#an array to store date of trip_updates files
trip_updates_paths = []
trip_updates_date_array = []
current_date = [""]
log_content = []

# used in log function to highlight in green color
def highlight_with_colour(text, colour):
    if(colour == "red"):
        return '\033[31m' + text + "\033[0m"
    elif(colour == "green"):
        return '\033[32m' + text + "\033[0m"

#time_stamp in log function
def log_timestamp(text, func_name, time, start_time=None):
    if start_time is None:  # used for specifying the start time
        print(highlight_with_colour(text, "green") + ' ' + highlight_with_colour(func_name + '(): ', "red") + str(time))
    else:  # used for specifying the finish time & time period
        print(highlight_with_colour(text, "green") + ' ' + highlight_with_colour(func_name + '(): ', "red") + str(time) + ', spent ' +
              highlight_with_colour(str(time - datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f')), "red"))

# log function can print the start & end time of other funcs
def log(func):
    def wrapper(*args, **kw):
        name = func.__name__
        start_time = datetime.datetime.now()
        log_timestamp('Start', name, start_time)  # record the start time for a function
        #return the function processed and show the time and period of the function
        return func(*args, **kw), \
               log_timestamp('Finish', name, datetime.datetime.now(), start_time=str(start_time))
    return wrapper

#Used to generate directories for Combined Data & Log
def create_dir(path):
    if os.path.exists('./' + path + '/'):
        pass
    else:
        os.mkdir('./' + path + '/')
    combined_path = './' + path + '/'
    return combined_path

# This function will be used to initialise all dates attached to the cleaned data file and put them all into the date array above
@log
def get_scheduled_data(scheduled_data_dir, data):
    log_content.append(str(datetime.datetime.now()) + " - Started reading Scheduled Data for date '" + current_date[0] + "'")
    found = False
    for root, dirs, files in os.walk(scheduled_data_dir):
        print('scheduled_data_dir: ', dirs)
        for folder_name in dirs:
            print('folder_name: ', folder_name)
            if current_date[0] in folder_name:
                print('current_date: ', current_date[0])
                file_path = join(root, folder_name)
                print('file_path: ', file_path)
                stop_times_file_path = file_path + "\stop_times.txt"
                stops_file_path = file_path + "\stops.txt"
                trips_file_path = file_path  +"\\trips.txt"
                routes_file_path = file_path + "\\routes.txt"
                calendar_file_path = file_path + "\calendar.txt"
                found = True
                print('find found: ', found)
        if not found:
            print("Cannot find the corresponding scheduled time for Train-TU-" + current_date[0])#change the date format
            break
    stop_times_data = pd.read_csv(stop_times_file_path, header=0, dtype=object,
                                  usecols=["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence","shape_dist_traveled"])
    stops_data = pd.read_csv(stops_file_path, header=0, dtype=object, usecols=["stop_id", "stop_name","parent_station"])
    trips_data = pd.read_csv(trips_file_path, header=0, dtype=object, usecols=["route_id", "service_id","trip_id","trip_headsign","trip_short_name"])
    routes_data = pd.read_csv(routes_file_path, header=0, dtype=object, usecols=["route_id", "route_short_name","route_long_name",])
    calendar_data = pd.read_csv(calendar_file_path, header=0, dtype=object, usecols=["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"])
    stops_combined_data = stop_times_data.merge(stops_data, how='left', on='stop_id', validate='m:m', sort=False)
    trips_combined_data = stops_combined_data.merge(trips_data, how='left', on='trip_id', validate='m:1', sort=False)
    routes_combined_data = trips_combined_data.merge(routes_data, how='left', on='route_id', validate='m:1', sort=False)
    calendar_combined_data = routes_combined_data.merge(calendar_data, how='left', on='service_id', validate='m:1', sort=False)
    data.append(calendar_combined_data)  # data[1]
    log_content.append(
        str(datetime.datetime.now()) + " - Ended reading Scheduled Data for date '" + current_date[0] + "'")
@log
def save_data(modified_data, file_name, cleaned_path):  # save the data into a file with format filename--hour.csv
	cleaned_dir = create_dir(cleaned_path)
	filename = cleaned_dir + file_name + '-- Combined.csv'
	if os.path.exists(filename):  # if the file exists, write without header
		modified_data.to_csv(filename, index=False, header=False, mode='a', na_rep='NA')
	else:  # if the file not exists, write with header
		modified_data.to_csv(filename, index=False, header=True, mode='a', na_rep='NA')
	del modified_data
def main():
    if os.path.exists('./GTFS_Static_Combiner1/'):
        pass
    else:
        create_dir('GTFS_Static_Combiner1')
    if os.path.exists('./Log/'):
        pass
    else:
        create_dir('Log')
    pd.set_option('display.max_columns', None)
    data = []
    get_scheduled_data('./GTFS_Scheduled/', data)
    reordered_data = data[0]
    save_data(reordered_data, '2019-05', "./GTFS_Static_Combiner1/")
if __name__ == "__main__":
    main()
