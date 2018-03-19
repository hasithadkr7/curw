'''
Created on Mar 15, 2018
@author: hasitha
'''

from airflow.models import BaseOperator
from datetime import datetime, timedelta, time
from ordereddict import OrderedDict
import glob
import csv
import os


class RfToCsvOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(RfToCsvOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        rf_forecasted_days = 0
        rain_csv_file = 'DailyRain.csv'
        rf_dir_path = './INPUT/rf'
        kub_dir_path = './INPUT/kub'
        output_dir = './OUTPUT'
        print 'executing {}'.__str__()
        rf_to_csv_convert(rf_forecasted_days, rain_csv_file, rf_dir_path, kub_dir_path, output_dir)


def rf_to_csv_convert():
    rf_forecasted_days = 0
    rain_csv_file = 'DailyRain.csv'
    rf_dir_path = './INPUT/rf'
    kub_dir_path = './INPUT/kub'
    output_dir = './OUTPUT'
    date = ''
    time = ''
    start_date = ''
    start_time = ''
    tag = ''

    upper_catchment_weights = {
        'Daraniyagala': 0.146828,  # 2
        'Glencourse': 0.208938,  # 3
        'Hanwella': 0.078722,  # 4
        'Holombuwa': 0.163191,  # 5
        'Kitulgala': 0.21462,  # 6
        'Norwood': 0.187701  # 7
    }
    upper_catchments = upper_catchment_weights.keys()

    lower_catchment_weights = {
        'Colombo': 1
    }
    lower_catchments = lower_catchment_weights.keys()

    kelani_upper_basin_weights = {
        'mean-rf': 1
    }
    kelani_upper_basin = kelani_upper_basin_weights.keys()

    # Default run for current day
    model_state = datetime.datetime.now() - timedelta(days=9)
    if date:
        model_state = datetime.datetime.strptime(date, '%Y-%m-%d')
    date = model_state.strftime("%Y-%m-%d")
    if time:
        model_state = datetime.datetime.strptime('%s %s' % (date, time), '%Y-%m-%d %H:%M:%S')
    time = model_state.strftime("%H:%M:%S")
    # Set the RF forecast data available file name pattern
    rf_forecasted_date = datetime.datetime.strptime(date, '%Y-%m-%d') + datetime.timedelta(hours=rf_forecasted_days)
    rf_forecasted_date = rf_forecasted_date.strftime("%Y-%m-%d")

    if start_date:
        start_date_time = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    else:
        start_date_time = datetime.datetime.strptime(date, '%Y-%m-%d')
    start_date = start_date_time.strftime("%Y-%m-%d")

    if start_time:
        start_date_time = datetime.datetime.strptime('%s %s' % (start_date, start_time), '%Y-%m-%d %H:%M:%S')
    start_time = start_date_time.strftime("%H:%M:%S")

    print('Rf to csv startTime:', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print('Rf to csv run for', date, '@', time, tag)
    print('With Custom starting', start_date, '@', start_time, ' using RF data of ', rf_forecasted_date)

    # TODO: Do not use any more, using WRF generated KUB
    upper_theissen_values = OrderedDict()
    for catchment in upper_catchments:
        for filename in glob.glob(os.path.join(rf_dir_path, '%s-%s*.txt' % (catchment, rf_forecasted_date))):
            print('Start Operating on (Upper) ', filename)
            csv_catchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csv_catchment = list(csv_catchment)
            for row in csv_catchment:
                d = datetime.strftime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in upper_theissen_values:
                    upper_theissen_values[key] = 0
                upper_theissen_values[key] += float(row[1].strip(' \t')) * upper_catchment_weights[catchment]

    # TODO: Need to be replace by retrieving data from database
    kelani_upper_basin_values = OrderedDict()
    for catchment in kelani_upper_basin:
        for filename in glob.glob(os.path.join(kub_dir_path, catchment + '-' + rf_forecasted_date + '*.txt')):
            print('Start Operating on (Kelani Upper Basin) ', filename)
            csv_catchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csv_catchment = list(csv_catchment)
            for row in csv_catchment:
                # print(row[0].replace('_', ' '), row[1].strip(' \t'))
                d = datetime.datetime.strptime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in kelani_upper_basin_values:
                    kelani_upper_basin_values[key] = 0
                kelani_upper_basin_values[key] += float(row[1].strip(' \t')) * kelani_upper_basin_weights[catchment]

    # TODO: Need to be replace by using KLB-Mean generate by WRF
    # TODO: Get data from database directly
    lower_thessian_values = OrderedDict()
    for lower_catchment in lower_catchments:
        for filename in glob.glob(os.path.join(rf_dir_path, lower_catchment + '-' + rf_forecasted_date + '*.txt')):
            print('Start Operating on (Lower) ', filename)
            csv_catchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csv_catchment = list(csv_catchment)
            for row in csv_catchment:
                # print(row[0].replace('_', ' '), row[1].strip(' \t'))
                d = datetime.datetime.strptime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in lower_thessian_values:
                    lower_thessian_values[key] = 0
                lower_thessian_values[key] += float(row[1].strip(' \t')) * lower_catchment_weights[lower_catchment]

    # TODO: Get Observed Data

    print('Finished processing files. Start Writing Theissen polygon avg in to CSV')

    file_name = rain_csv_file.rsplit('.', 1)
    file_name = '{name}-{date}{tag}.{extention}'.format(name=file_name[0], date=date, tag='.' + tag if tag else '', extention=file_name[1])
    rain_csv_file_path = os.path.join(output_dir, file_name)
    csv_writer = csv.writer(open(rain_csv_file_path, 'w'), delimiter=',', quotechar='|')
    # Write Metadata https://publicwiki.deltares.nl/display/FEWSDOC/CSV
    csv_writer.writerow(['Location Names', 'Awissawella', 'Colombo'])
    csv_writer.writerow(['Location Ids', 'Awissawella', 'Colombo'])
    csv_writer.writerow(['Time', 'Rainfall', 'Rainfall'])

    # TODO:Insert available observed data

    # Iterate through each timestamp
    for avg in upper_theissen_values:
        print(avg, upper_theissen_values[avg], lower_thessian_values[avg])
        csv_writer.writerow([d.strftime('%Y-%m-%d %H:%M:%S'), "%.2f" % kelani_upper_basin_values[avg],
                                "%.2f" % lower_thessian_values[avg]])
