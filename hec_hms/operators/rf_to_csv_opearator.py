'''
Created on Mar 15, 2018
@author: hasitha
'''

from airflow.models import BaseOperator
from datetime import datetime, timedelta
from airflow.utils import logging
from airflow.utils.decorators import apply_defaults
from ordereddict import OrderedDict
import glob
import csv
import os
from hec_hms import constants as configs

log = logging.getLogger(__name__)


class RfToCsvOperator(BaseOperator):
    @apply_defaults
    def __init__(self, start_date, start_time, *args, **kwargs):
        self.start_date = start_date
        self.start_time = start_time
        super(RfToCsvOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("RfToCsvOperator...")
        log.info('start_date: %s', self.start_date)
        rf_to_csv_convert(self.start_date,self.start_date),
        log.info('Rf data has converted into CSV format.')


def rf_to_csv_convert(startDate, startTime):
    RF_FORECASTED_DAYS = configs.RF_FORECASTED_DAYS
    RAIN_CSV_FILE = configs.RAIN_CSV_FILE
    RF_DIR_PATH = configs.RF_DIR_PATH
    KUB_DIR_PATH = configs.KUB_DIR_PATH
    OUTPUT_DIR = configs.OUTPUT_DIR
    date = ''
    time = ''
    tag = ''
    UPPER_CATCHMENT_WEIGHTS = {
        # 'Attanagalla'   : 1/7,    # 1
        'Daraniyagala': 0.146828,  # 2
        'Glencourse': 0.208938,  # 3
        'Hanwella': 0.078722,  # 4
        'Holombuwa': 0.163191,  # 5
        'Kitulgala': 0.21462,  # 6
        'Norwood': 0.187701  # 7
    }
    UPPER_CATCHMENTS = UPPER_CATCHMENT_WEIGHTS.keys()

    KELANI_UPPER_BASIN_WEIGHTS = {
        'mean-rf': 1
    }
    KELANI_UPPER_BASIN = KELANI_UPPER_BASIN_WEIGHTS.keys()

    LOWER_CATCHMENT_WEIGHTS = {
        'Colombo': 1
    }
    LOWER_CATCHMENTS = LOWER_CATCHMENT_WEIGHTS.keys()

    # Default run for current day
    modelState = datetime.now() - timedelta(days=10)
    # print(modelState)
    if date:
        modelState = datetime.strptime(date, '%Y-%m-%d')
    date = modelState.strftime("%Y-%m-%d")
    if time:
        modelState = datetime.strptime('%s %s' % (date, time), '%Y-%m-%d %H:%M:%S')
    time = modelState.strftime("%H:%M:%S")
    # Set the RF forecast data available file name pattern
    rfForecastedDate = datetime.strptime(date, '%Y-%m-%d') + timedelta(hours=RF_FORECASTED_DAYS)
    rfForecastedDate = rfForecastedDate.strftime("%Y-%m-%d")

    startDateTime = datetime.now()
    if startDate:
        startDateTime = datetime.strptime(startDate, '%Y-%m-%d')
    else:
        startDateTime = datetime.strptime(date, '%Y-%m-%d')
    startDate = startDateTime.strftime("%Y-%m-%d")

    if startTime:
        startDateTime = datetime.strptime('%s %s' % (startDate, startTime), '%Y-%m-%d %H:%M:%S')
    startTime = startDateTime.strftime("%H:%M:%S")

    # print('RFTOCSV startTime:', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # print(' RFTOCSV run for', date, '@', time, tag)
    # print(' With Custom starting', startDate, '@', startTime, ' using RF data of ', rfForecastedDate)

    # TODO: Do not use any more, using WRF generated KUB
    UPPER_THEISSEN_VALUES = OrderedDict()

    # print('UPPER_CATCHMENTS:',UPPER_CATCHMENTS)
    # print('rfForecastedDate:',rfForecastedDate)
    # ('RF_DIR_PATH:',RF_DIR_PATH)

    for catchment in UPPER_CATCHMENTS:
        for filename in glob.glob(os.path.join(RF_DIR_PATH, '%s-%s*.txt' % (catchment, rfForecastedDate))):
            # print('start operating on (upper) ', filename)
            csvcatchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csvcatchment = list(csvcatchment)
            # print('csvcatchment:',csvcatchment)
            for row in csvcatchment:
                DateTimeStr = row[0].replace('_', ' ')
                d = datetime.strptime(DateTimeStr, '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                # print('key:',key)
                if key not in UPPER_THEISSEN_VALUES:
                    UPPER_THEISSEN_VALUES[key] = 0
                UPPER_THEISSEN_VALUES[key] += float(row[1].strip(' \t')) * UPPER_CATCHMENT_WEIGHTS[catchment]

    # print('UPPER_THEISSEN_VALUES:',UPPER_THEISSEN_VALUES)
    # print('KELANI_UPPER_BASIN:',KELANI_UPPER_BASIN)
    # print('KUB_DIR_PATH:',KUB_DIR_PATH)
    KELANI_UPPER_BASIN_VALUES = OrderedDict()
    for catchment in KELANI_UPPER_BASIN:
        # print('KELANI_UPPER_BASIN:',KELANI_UPPER_BASIN)
        for filename in glob.glob(os.path.join(KUB_DIR_PATH, catchment + '-' + rfForecastedDate + '*.txt')):
            # print('Start Operating on (Kelani Upper Basin) ', filename)
            csvCatchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csvCatchment = list(csvCatchment)
            for row in csvCatchment:
                # print(row[0].replace('_', ' '), row[1].strip(' \t'))
                d = datetime.strptime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in KELANI_UPPER_BASIN_VALUES:
                    KELANI_UPPER_BASIN_VALUES[key] = 0
                KELANI_UPPER_BASIN_VALUES[key] += float(row[1].strip(' \t')) * KELANI_UPPER_BASIN_WEIGHTS[catchment]

    # TODO: Need to be replace by using KLB-Mean generate by WRF
    # TODO: Get data from database directly
    print('LOWER_CATCHMENTS:', LOWER_CATCHMENTS)
    print('RF_DIR_PATH:', RF_DIR_PATH)
    LOWER_THEISSEN_VALUES = OrderedDict()
    for lowerCatchment in LOWER_CATCHMENTS:
        print('----------------------LOWER_CATCHMENTS:', LOWER_CATCHMENTS)
        for filename in glob.glob(os.path.join(RF_DIR_PATH, lowerCatchment + '-' + rfForecastedDate + '*.txt')):
            print('Start Operating on (Lower) ', filename)
            csvCatchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csvCatchment = list(csvCatchment)
            print('----------------------------csvCatchment:', csvCatchment)
            for row in csvCatchment:
                print(row[0].replace('_', ' '), row[1].strip(' \t'))
                d = datetime.strptime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in LOWER_THEISSEN_VALUES:
                    LOWER_THEISSEN_VALUES[key] = 0
                LOWER_THEISSEN_VALUES[key] += float(row[1].strip(' \t')) * LOWER_CATCHMENT_WEIGHTS[lowerCatchment]
    print('LOWER_THEISSEN_VALUES:', LOWER_THEISSEN_VALUES)
    KLB_Timeseries = []
    if len(KLB_Timeseries) > 0:
        print(KLB_Timeseries)
        print('KLB_Timeseries::', len(KLB_Timeseries), KLB_Timeseries[0], KLB_Timeseries[-1])
    else:
        print('No data found for KLB Obs timeseries: ', KLB_Timeseries)

    # print('Finished processing files. Start Writing Theissen polygon avg in to CSV')
    # print('UPPER_THEISSEN_VALUES:',UPPER_THEISSEN_VALUES)

    fileName = RAIN_CSV_FILE.rsplit('.', 1)
    fileName = '{name}-{date}{tag}.{extention}'.format(name=fileName[0], date=date, tag='.' + tag if tag else '',
                                                       extention=fileName[1])
    # print('OUTPUT_DIR:',OUTPUT_DIR)
    # print('fileName:',fileName)
    RAIN_CSV_FILE_PATH = os.path.join(OUTPUT_DIR, fileName)
    # print('RAIN_CSV_FILE_PATH:',RAIN_CSV_FILE_PATH)
    csvWriter = csv.writer(open(RAIN_CSV_FILE_PATH, 'w'), delimiter=',', quotechar='|')
    # Write Metadata https://publicwiki.deltares.nl/display/FEWSDOC/CSV
    csvWriter.writerow(['Location Names', 'Awissawella', 'Colombo'])
    csvWriter.writerow(['Location Ids', 'Awissawella', 'Colombo'])
    csvWriter.writerow(['Time', 'Rainfall', 'Rainfall'])

    # Iterate through each timestamp
    # print('UPPER_THEISSEN_VALUES:',UPPER_THEISSEN_VALUES)
    # print('LOWER_THEISSEN_VALUES:',LOWER_THEISSEN_VALUES)
    for avg in UPPER_THEISSEN_VALUES:
        print(avg, UPPER_THEISSEN_VALUES[avg], LOWER_THEISSEN_VALUES[avg])
        d = datetime.fromtimestamp(avg)
        csvWriter.writerow([d.strftime('%Y-%m-%d %H:%M:%S'), "%.2f" % KELANI_UPPER_BASIN_VALUES[avg],
                            "%.2f" % LOWER_THEISSEN_VALUES[avg]])
    print('Completed ', RF_DIR_PATH, ' to ', RAIN_CSV_FILE_PATH)
