import pandas as pd
import os
import datetime
from os.path import join
from decimal import Decimal

__author__ = 'Jianqing Wu'


def highlight_blue(text):  # used in log function to highlight in green color
	blue = '\033[34m' + text + "\033[0m"
	return blue


def highlight_red(text):  # used in log function to highlight in red color
	red = '\033[31m' + text + "\033[0m"
	return red

def log(func):  # log function can print the start & end time of other funcs
    def wrapper(*args, **kw):
        name = func.__name__
        start_time = datetime.datetime.now()
        time_stamp('Start', name, start_time)  # record the start time for a function

        return func(*args, **kw), \
               time_stamp('Finish', name, datetime.datetime.now(), start_time=str(start_time))
    return wrapper

def create_dir(path):  # Used to generate directories for Cleaned Data & Log
	if os.path.exists('./' + path + '/'):
		pass
	else:
		os.mkdir('./' + path + '/')
	cleaned_path = './' + path + '/'
	return cleaned_path

@log
def get_raw_data(raw_data_dir):
	walk_files = os.walk(raw_data_dir)
	for root, dirs, files in walk_files:  # walk through the directory inspecting if any data exists
		if len(files) == 0:
			print('There is no raw data')
		else:  # raw data exists, then next step
			all_files = data_processing(root, files)
	return all_files[0]

@log
def data_processing(root, files):
	files_info = {}
	for file_name in files:
		if file_name.find('.csv') == -1:  # Only set the files with '.csv' as targets
			continue
		file_path = join(root, file_name)
		file_size = str(Decimal(os.path.getsize(file_path) / 1048576).quantize(Decimal('0.00'))) + 'Mb'
		files_info[file_name] = {'Path': file_path, 'Size': file_size}
	return files_info

def time_stamp(text, func_name, time, start_time=None):
	if start_time is None:  # used for specifying the start time
		print(highlight_blue(text) + ' ' + highlight_red(func_name + '(): ') + str(time))
	else:  # used for specifying the finish time & time period
		print(highlight_blue(text) + ' ' + highlight_red(func_name + '(): ') + str(time) + ', spent ' +
			  highlight_red(str(time - datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f'))))

@log
def csv_processing(file_name, target_file):
    start_clean = datetime.datetime.now()
    start = datetime.datetime.now()
    raw_data = pd.read_csv(target_file, names =['Index_Number', 'trip_id','Schedule_Relationship','Route_ID', 'Fetch_Time', 'Arrival_Delay','Actual_Arrival_Time','Departure_Delay','Actual_Departure_Time','stop_id','Stop_Schedule_Relationship'],dtype = object)
    end = datetime.datetime.now()
    loading_time = end - start
    print('Loading data spent: ', loading_time)
    raw_data_number = len(raw_data)
    print('The number of raw data rows: ', raw_data_number)
    order_start = datetime.datetime.now()
    order_end = datetime.datetime.now()
    ordering_time = order_end- order_start
    print('Ordering data spent: ', ordering_time)
    duplicated_start = datetime.datetime.now()
    data_duplicated = raw_data.drop_duplicates(subset={'trip_id', 'stop_id'}, keep='last')
    duplicated_end =datetime.datetime.now()
    duplicated_time = duplicated_end - duplicated_start
    print('Dropping duplicated data spent: ', duplicated_time)
    cleaning_time = datetime.datetime.now() - start_clean
    valid_data = len(data_duplicated)
    print('The number of valid data rows: ', valid_data)
    start_save = datetime.datetime.now()
    save_data(data_duplicated, file_name, 'Train_TU')
    end_save = datetime.datetime.now()
    save_data_time = end_save - start_save
    file_info = [loading_time,raw_data_number, ordering_time, duplicated_time, cleaning_time, valid_data, save_data_time]
    return file_info

@log
def save_data(divided_data, file_name, cleaned_path):  # save the data into a file with format filename--hour.csv
	if len(divided_data) != 0:
		file_name = file_name.split('.')[0]
		cleaned_dir = create_dir(cleaned_path)
		filename = cleaned_dir + file_name + '.csv'
		if os.path.exists(filename):  # if the file exists, write without header
			pass
		else:  # if the file not exists, write with header
			divided_data.to_csv(filename, index=False, header=True, mode='a', na_rep='NA')
	del divided_data

def main():
    if os.path.exists('./Train_TU/'):
        pass
    else:
        create_dir('Train_TU')
    if os.path.exists('./Train_TU_Log/'):
        pass
    else:
        create_dir('Train_TU_Log')
    all_files = get_raw_data('E:\Train_TU_Test')[0]
    summary = {}
    for key, value in all_files.items():
        print('---------------------------------')
        print('File name: ' + key + ', Path: ' + value['Path'] + ', Size: ' + value['Size'])
        start_time = datetime.datetime.now()
        #		loading_time, data_length_without_duplicate, valid_data, cleaning_time, save_data_time = ind_csv_processing(key, value['Path'])
        file_info = csv_processing(key, value['Path'])
        total_time = datetime.datetime.now()
        summary[key] = [str(total_time - start_time), value['Size']]  # used for summary & log
        with open('./Train_TU_Log/Log.txt'.format(key), 'a+', encoding='utf8', newline="") as fo:  # save summary into log
            summary_info = 'Processing ' + key + '(' + summary[key][1] + ')' + '_spent ' + summary[key][
                0] + '\r\n\t' + \
                           'Loading time: ' + str(file_info[0][0]) + '\r\n\t' + \
                           'Raw data: ' + str(file_info[0][1]) + '\r\n\t' + \
                           'Ordering data: ' + str(file_info[0][2]) + '\r\n\t' + \
                           'Dropping duplicated data: ' + str(file_info[0][3]) + '\r\n\t' + \
                           'Data cleaning spent: ' + str(file_info[0][4]) + '\r\n\t' + \
                           'Valid data: ' + str(file_info[0][5]) + '\r\n\t' + \
                           'Data saving spent: ' + str(file_info[0][6]) + '\r\n'
            print(summary_info)
            fo.write(summary_info)
    print('-------------------------------')
    print('Processing Log: ')
    for i in summary:
        print('Processing ' + highlight_red(i) + '(' + summary[i][1] + ')' + '_spent ' + highlight_red(
            summary[i][0]))

if __name__ == "__main__":
    main()
