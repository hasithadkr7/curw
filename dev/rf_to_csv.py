
import csv
import datetime
import glob
import json
import os
import traceback
from collections import OrderedDict


try:
    CONFIG = json.loads(open('CONFIG.json').read())
    # print('Config :: ', CONFIG)
    RF_FORECASTED_DAYS = 0
    RAIN_CSV_FILE = 'DailyRain.csv'
    RF_DIR_PATH = './WRF/RF/'
    KUB_DIR_PATH = './WRF/kelani-upper-basin'
    OUTPUT_DIR = './OUTPUT'
    # Kelani Upper Basin
    # KUB_OBS_ID = 'b0e008522be904bcf71e290b3b0096b33c3e24d9b623dcbe7e58e7d1cc82d0db'
    KUB_OBS_ID = 'fb575cb25f1e3d3a07c84513ea6a91c8f2fb98454df1a432518ab98ad7182861'  # wrf0, kub_mean, 0-d
    # Kelani Lower Basin
    # KLB_OBS_ID = '3fb96706de7433ba6aff4936c9800a28c9599efd46cbc9216a5404aab812d76a'
    KLB_OBS_ID = '69c464f749b36d9e55e461947238e7ed809c2033e75ae56234f466eec00aee35'  # wrf0, klb_mean, 0-d

    MYSQL_HOST = "localhost"
    MYSQL_USER = "root"
    MYSQL_DB = "curw"
    MYSQL_PASSWORD = ""

    if 'RF_FORECASTED_DAYS' in CONFIG:
        RF_FORECASTED_DAYS = CONFIG['RF_FORECASTED_DAYS']
    if 'RAIN_CSV_FILE' in CONFIG:
        RAIN_CSV_FILE = CONFIG['RAIN_CSV_FILE']
    if 'RF_DIR_PATH' in CONFIG:
        RF_DIR_PATH = CONFIG['RF_DIR_PATH']
    if 'KUB_DIR_PATH' in CONFIG:
        KUB_DIR_PATH = CONFIG['KUB_DIR_PATH']
    if 'OUTPUT_DIR' in CONFIG:
        OUTPUT_DIR = CONFIG['OUTPUT_DIR']

    if 'MYSQL_HOST' in CONFIG:
        MYSQL_HOST = CONFIG['MYSQL_HOST']
    if 'MYSQL_USER' in CONFIG:
        MYSQL_USER = CONFIG['MYSQL_USER']
    if 'MYSQL_DB' in CONFIG:
        MYSQL_DB = CONFIG['MYSQL_DB']
    if 'MYSQL_PASSWORD' in CONFIG:
        MYSQL_PASSWORD = CONFIG['MYSQL_PASSWORD']

    date = ''
    time = ''
    startDate = ''
    startTime = ''
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
    modelState = datetime.datetime.now()
    if date:
        modelState = datetime.datetime.strptime(date, '%Y-%m-%d')
    date = modelState.strftime("%Y-%m-%d")
    if time:
        modelState = datetime.datetime.strptime('%s %s' % (date, time), '%Y-%m-%d %H:%M:%S')
    time = modelState.strftime("%H:%M:%S")
    # Set the RF forecast data available file name pattern
    rfForecastedDate = datetime.datetime.strptime(date, '%Y-%m-%d') + datetime.timedelta(hours=RF_FORECASTED_DAYS)
    rfForecastedDate = rfForecastedDate.strftime("%Y-%m-%d")

    startDateTime = datetime.datetime.now()
    if startDate:
        startDateTime = datetime.datetime.strptime(startDate, '%Y-%m-%d')
    else:
        startDateTime = datetime.datetime.strptime(date, '%Y-%m-%d')
    startDate = startDateTime.strftime("%Y-%m-%d")

    if startTime:
        startDateTime = datetime.datetime.strptime('%s %s' % (startDate, startTime), '%Y-%m-%d %H:%M:%S')
    startTime = startDateTime.strftime("%H:%M:%S")

    print('RFTOCSV startTime:', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print(' RFTOCSV run for', date, '@', time, tag)
    print(' With Custom starting', startDate, '@', startTime, ' using RF data of ', rfForecastedDate)

    # TODO: Do not use any more, using WRF generated KUB
    UPPER_THEISSEN_VALUES = OrderedDict()
    for catchment in UPPER_CATCHMENTS:
        for filename in glob.glob(os.path.join(RF_DIR_PATH, '%s-%s*.txt' % (catchment, rfForecastedDate))):
            print('Start Operating on (Upper) ', filename)
            csvCatchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csvCatchment = list(csvCatchment)
            for row in csvCatchment:
                # print(row[0].replace('_', ' '), row[1].strip(' \t'))
                d = datetime.datetime.strptime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in UPPER_THEISSEN_VALUES:
                    UPPER_THEISSEN_VALUES[key] = 0
                UPPER_THEISSEN_VALUES[key] += float(row[1].strip(' \t')) * UPPER_CATCHMENT_WEIGHTS[catchment]

    # TODO: Need to be replace by retrieving data from database
    KELANI_UPPER_BASIN_VALUES = OrderedDict()
    for catchment in KELANI_UPPER_BASIN:
        for filename in glob.glob(os.path.join(KUB_DIR_PATH, catchment + '-' + rfForecastedDate + '*.txt')):
            print('Start Operating on (Kelani Upper Basin) ', filename)
            csvCatchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csvCatchment = list(csvCatchment)
            for row in csvCatchment:
                # print(row[0].replace('_', ' '), row[1].strip(' \t'))
                d = datetime.datetime.strptime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in KELANI_UPPER_BASIN_VALUES:
                    KELANI_UPPER_BASIN_VALUES[key] = 0
                KELANI_UPPER_BASIN_VALUES[key] += float(row[1].strip(' \t')) * KELANI_UPPER_BASIN_WEIGHTS[catchment]

    # TODO: Need to be replace by using KLB-Mean generate by WRF
    # TODO: Get data from database directly
    LOWER_THEISSEN_VALUES = OrderedDict()
    for lowerCatchment in LOWER_CATCHMENTS:
        for filename in glob.glob(os.path.join(RF_DIR_PATH, lowerCatchment + '-' + rfForecastedDate + '*.txt')):
            print('Start Operating on (Lower) ', filename)
            csvCatchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csvCatchment = list(csvCatchment)
            for row in csvCatchment:
                # print(row[0].replace('_', ' '), row[1].strip(' \t'))
                d = datetime.datetime.strptime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in LOWER_THEISSEN_VALUES:
                    LOWER_THEISSEN_VALUES[key] = 0
                LOWER_THEISSEN_VALUES[key] += float(row[1].strip(' \t')) * LOWER_CATCHMENT_WEIGHTS[lowerCatchment]

    KUB_Timeseries = []
    if len(KUB_Timeseries) > 0:
        # print(KUB_Timeseries)
        print('KUB_Timeseries::', len(KUB_Timeseries), KUB_Timeseries[0], KUB_Timeseries[-1])
    else:
        print('No data found for KUB Obs timeseries: ', KUB_Timeseries)
    KLB_Timeseries = []
    if len(KLB_Timeseries) > 0:
        # print(KLB_Timeseries)
        print('KLB_Timeseries::', len(KLB_Timeseries), KLB_Timeseries[0], KLB_Timeseries[-1])
    else:
        print('No data found for KLB Obs timeseries: ', KLB_Timeseries)

    print('Finished processing files. Start Writing Theissen polygon avg in to CSV')
    # print(UPPER_THEISSEN_VALUES)

    fileName = RAIN_CSV_FILE.rsplit('.', 1)
    fileName = '{name}-{date}{tag}.{extention}'.format(name=fileName[0], date=date, tag='.' + tag if tag else '',
                                                       extention=fileName[1])
    RAIN_CSV_FILE_PATH = os.path.join(OUTPUT_DIR, fileName)
    csvWriter = csv.writer(open(RAIN_CSV_FILE_PATH, 'w'), delimiter=',', quotechar='|')
    # Write Metadata https://publicwiki.deltares.nl/display/FEWSDOC/CSV
    csvWriter.writerow(['Location Names', 'Awissawella', 'Colombo'])
    csvWriter.writerow(['Location Ids', 'Awissawella', 'Colombo'])
    csvWriter.writerow(['Time', 'Rainfall', 'Rainfall'])

    # Insert available observed data
    lastObsDateTime = startDateTime
    for kub_tt in KUB_Timeseries:
        # look for same time value in Kelani Basin
        klb_tt = kub_tt  # TODO: Better to replace with missing ???
        for sub_tt in KLB_Timeseries:
            if sub_tt[0] == kub_tt[0]:
                klb_tt = sub_tt
                break

        csvWriter.writerow([kub_tt[0].strftime('%Y-%m-%d %H:%M:%S'), "%.2f" % kub_tt[1], "%.2f" % klb_tt[1]])
        lastObsDateTime = kub_tt[0]

    # Iterate through each timestamp
    for avg in UPPER_THEISSEN_VALUES:
        # print(avg, UPPER_THEISSEN_VALUES[avg], LOWER_THEISSEN_VALUES[avg])
        d = datetime.datetime.fromtimestamp(avg)
        if d > lastObsDateTime:
            csvWriter.writerow([d.strftime('%Y-%m-%d %H:%M:%S'), "%.2f" % KELANI_UPPER_BASIN_VALUES[avg],
                                "%.2f" % LOWER_THEISSEN_VALUES[avg]])

except ValueError:
    raise ValueError("Incorrect data format, should be YYYY-MM-DD")
except Exception as e:
    print(e)
    traceback.print_exc()
finally:
    print('Completed ', RF_DIR_PATH, ' to ', RAIN_CSV_FILE_PATH)
