import csv
from datetime import datetime, timedelta
import glob
import os
from collections import OrderedDict


def usage():
    usage_text = """
    Usage: ./CSVTODAT.py [-d YYYY-MM-DD] [-t HH:MM:SS] [-h]
    
    -h  --help          Show usage
    -d  --date          Date in YYYY-MM-DD. Default is current date.
    -t  --time          Time in HH:MM:SS. Default is current time.
        --start-date    Start date of timeseries which need to run the forecast in YYYY-MM-DD format. Default is same as -d(date).
        --start-time    Start time of timeseries which need to run the forecast in HH:MM:SS format. Default is same as -t(date).
    -T  --tag           Tag to differential simultaneous Forecast Runs E.g. wrf1, wrf2 ...
        --wrf-rf        Path of WRF Rf(Rainfall) Directory. Otherwise using the `RF_DIR_PATH` from CONFIG.json
        --wrf-kub       Path of WRF kelani-upper-basin(KUB) Directory. Otherwise using the `KUB_DIR_PATH` from CONFIG.json
    """
    print(usage_text)


def rf_to_csv_convert():
    RF_FORECASTED_DAYS = 0
    RAIN_CSV_FILE = 'DailyRain.csv'
    RF_DIR_PATH = './INPUT/rf'
    KUB_DIR_PATH = './INPUT/kub'
    OUTPUT_DIR = './OUTPUT'
    # Kelani Upper Basin
    # KUB_OBS_ID = 'b0e008522be904bcf71e290b3b0096b33c3e24d9b623dcbe7e58e7d1cc82d0db'
    KUB_OBS_ID = 'fb575cb25f1e3d3a07c84513ea6a91c8f2fb98454df1a432518ab98ad7182861'  # wrf0, kub_mean, 0-d
    # Kelani Lower Basin
    # KLB_OBS_ID = '3fb96706de7433ba6aff4936c9800a28c9599efd46cbc9216a5404aab812d76a'
    KLB_OBS_ID = '69c464f749b36d9e55e461947238e7ed809c2033e75ae56234f466eec00aee35'  # wrf0, klb_mean, 0-d

    date = ''
    time = ''
    startDate = '2018-03-10'
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
    modelState = datetime.now()-timedelta(days=9)
    #print(modelState)
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

    #print('RFTOCSV startTime:', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #print(' RFTOCSV run for', date, '@', time, tag)
    #print(' With Custom starting', startDate, '@', startTime, ' using RF data of ', rfForecastedDate)

    # TODO: Do not use any more, using WRF generated KUB
    UPPER_THEISSEN_VALUES = OrderedDict()

    #print('UPPER_CATCHMENTS:',UPPER_CATCHMENTS)
    #print('rfForecastedDate:',rfForecastedDate)
    #('RF_DIR_PATH:',RF_DIR_PATH)

    for catchment in UPPER_CATCHMENTS:
        for filename in glob.glob(os.path.join(RF_DIR_PATH, '%s-%s*.txt' % (catchment, rfForecastedDate))):
            #print('start operating on (upper) ', filename)
            csvcatchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csvcatchment = list(csvcatchment)
            #print('csvcatchment:',csvcatchment)
            for row in csvcatchment:
                DateTimeStr = row[0].replace('_', ' ')
                d = datetime.strptime(DateTimeStr, '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                #print('key:',key)
                if key not in UPPER_THEISSEN_VALUES:
                    UPPER_THEISSEN_VALUES[key] = 0
                UPPER_THEISSEN_VALUES[key] += float(row[1].strip(' \t')) * UPPER_CATCHMENT_WEIGHTS[catchment]

    #print('UPPER_THEISSEN_VALUES:',UPPER_THEISSEN_VALUES)
    #print('KELANI_UPPER_BASIN:',KELANI_UPPER_BASIN)
    #print('KUB_DIR_PATH:',KUB_DIR_PATH)
    KELANI_UPPER_BASIN_VALUES = OrderedDict()
    for catchment in KELANI_UPPER_BASIN:
        #print('KELANI_UPPER_BASIN:',KELANI_UPPER_BASIN)
        for filename in glob.glob(os.path.join(KUB_DIR_PATH, catchment + '-' + rfForecastedDate + '*.txt')):
            #print('Start Operating on (Kelani Upper Basin) ', filename)
            csvCatchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csvCatchment = list(csvCatchment)
            for row in csvCatchment:
                #print(row[0].replace('_', ' '), row[1].strip(' \t'))
                d = datetime.strptime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in KELANI_UPPER_BASIN_VALUES:
                    KELANI_UPPER_BASIN_VALUES[key] = 0
                KELANI_UPPER_BASIN_VALUES[key] += float(row[1].strip(' \t')) * KELANI_UPPER_BASIN_WEIGHTS[catchment]

    # TODO: Need to be replace by using KLB-Mean generate by WRF
    # TODO: Get data from database directly
    print('LOWER_CATCHMENTS:',LOWER_CATCHMENTS)
    print('RF_DIR_PATH:',RF_DIR_PATH)
    LOWER_THEISSEN_VALUES = OrderedDict()
    for lowerCatchment in LOWER_CATCHMENTS:
        print('----------------------LOWER_CATCHMENTS:',LOWER_CATCHMENTS)
        for filename in glob.glob(os.path.join(RF_DIR_PATH, lowerCatchment + '-' + rfForecastedDate + '*.txt')):
            print('Start Operating on (Lower) ', filename)
            csvCatchment = csv.reader(open(filename, 'r'), delimiter=' ', skipinitialspace=True)
            csvCatchment = list(csvCatchment)
            print('----------------------------csvCatchment:',csvCatchment)
            for row in csvCatchment:
                print(row[0].replace('_', ' '), row[1].strip(' \t'))
                d = datetime.strptime(row[0].replace('_', ' '), '%Y-%m-%d %H:%M:%S')
                key = d.timestamp()
                if key not in LOWER_THEISSEN_VALUES:
                    LOWER_THEISSEN_VALUES[key] = 0
                LOWER_THEISSEN_VALUES[key] += float(row[1].strip(' \t')) * LOWER_CATCHMENT_WEIGHTS[lowerCatchment]
    print('LOWER_THEISSEN_VALUES:',LOWER_THEISSEN_VALUES)
    KLB_Timeseries = []
    if len(KLB_Timeseries) > 0:
        print(KLB_Timeseries)
        print('KLB_Timeseries::', len(KLB_Timeseries), KLB_Timeseries[0], KLB_Timeseries[-1])
    else:
        print('No data found for KLB Obs timeseries: ', KLB_Timeseries)

    #print('Finished processing files. Start Writing Theissen polygon avg in to CSV')
    #print('UPPER_THEISSEN_VALUES:',UPPER_THEISSEN_VALUES)

    fileName = RAIN_CSV_FILE.rsplit('.', 1)
    fileName = '{name}-{date}{tag}.{extention}'.format(name=fileName[0], date=date, tag='.' + tag if tag else '',
                                                       extention=fileName[1])
    #print('OUTPUT_DIR:',OUTPUT_DIR)
    #print('fileName:',fileName)
    RAIN_CSV_FILE_PATH = os.path.join(OUTPUT_DIR, fileName)
    #print('RAIN_CSV_FILE_PATH:',RAIN_CSV_FILE_PATH)
    csvWriter = csv.writer(open(RAIN_CSV_FILE_PATH, 'w'), delimiter=',', quotechar='|')
    # Write Metadata https://publicwiki.deltares.nl/display/FEWSDOC/CSV
    csvWriter.writerow(['Location Names', 'Awissawella', 'Colombo'])
    csvWriter.writerow(['Location Ids', 'Awissawella', 'Colombo'])
    csvWriter.writerow(['Time', 'Rainfall', 'Rainfall'])


    # Iterate through each timestamp
    #print('UPPER_THEISSEN_VALUES:',UPPER_THEISSEN_VALUES)
    #print('LOWER_THEISSEN_VALUES:',LOWER_THEISSEN_VALUES)
    for avg in UPPER_THEISSEN_VALUES:
        print(avg, UPPER_THEISSEN_VALUES[avg], LOWER_THEISSEN_VALUES[avg])
        d = datetime.fromtimestamp(avg)
        csvWriter.writerow([d.strftime('%Y-%m-%d %H:%M:%S'), "%.2f" % KELANI_UPPER_BASIN_VALUES[avg],
                                "%.2f" % LOWER_THEISSEN_VALUES[avg]])
    print('Completed ', RF_DIR_PATH, ' to ', RAIN_CSV_FILE_PATH)
