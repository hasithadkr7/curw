'''
Created on Mar 15, 2018
@author: hasitha
'''
import java, csv, sys, datetime
import os
from hec.script import MessageBox
from hec.heclib.dss import HecDss
from hec.heclib.util import HecTime
from hec.io import TimeSeriesContainer
from optparse import OptionParser

sys.path.append("./simplejson-2.5.2")

try:
    try:
        print 'Jython version: ', sys.version
        num_metadata_lines = 3
        hec_hms_model_dir = './2008_2_Events'
        dss_input_file = './2008_2_Events/2008_2_Events_force.dss'
        rain_csv_file = 'DailyRain.csv'
        output_dir = './OUTPUT'

        date = ''
        time = ''
        start_date_ts = ''
        start_time_ts = ''
        tag = ''

        # Passing Commandline Options to Jython. Not same as getopt in python.
        # Ref: http://www.jython.org/jythonbook/en/1.0/Scripting.html#parsing-commandline-options
        # Doc : https://docs.python.org/2/library/optparse.html
        parser = OptionParser(description='Upload CSV data into HEC-HMS DSS storage')
        # ERROR: Unable to use `-d` or `-D` option with OptionParser
        parser.add_option("--date", help="Date in YYYY-MM. Default is current date.")
        parser.add_option("--time", help="Time in HH:MM:SS. Default is current time.")
        parser.add_option("--start-date",
                          help="Start date of timeseries which need to run the forecast in YYYY-MM-DD format. Default is same as -d(date).")
        parser.add_option("--start-time",
                          help="Start time of timeseries which need to run the forecast in HH:MM:SS format. Default is same as -t(date).")
        parser.add_option("-T", "--tag", help="Tag to differential simultaneous Forecast Runs E.g. wrf1, wrf2 ...")
        parser.add_option("--hec-hms-model-dir",
                          help="Path of hec_hms_model_dir directory. Otherwise using the `hec_hms_model_dir` from CONFIG.json")

        (options, args) = parser.parse_args()
        print 'Commandline Options:', options

        if options.date:
            date = options.date
        if options.time:
            time = options.time
        if options.start_date:
            start_date_ts = options.start_date
        if options.start_time:
            start_time_ts = options.start_time
        if options.tag:
            tag = options.tag
        if options.hec_hms_model_dir:
            hec_hms_model_dir = options.hec_hms_model_dir
            # Reconstruct dss_input_file path
            dss_file_name = dss_input_file.rsplit('/', 1)
            dss_input_file = os.path.join(hec_hms_model_dir, dss_file_name[-1])

        # Default run for current day
        model_state = datetime.datetime.now()
        if date:
            model_state = datetime.datetime.strptime(date, '%Y-%m-%d')
        date = model_state.strftime("%Y-%m-%d")
        if time:
            model_state = datetime.datetime.strptime('%s %s' % (date, time), '%Y-%m-%d %H:%M:%S')
        time = model_state.strftime("%H:%M:%S")

        if start_date_ts:
            start_date_time_ts = datetime.datetime.strptime(start_date_ts, '%Y-%m-%d')
        else:
            start_date_time_ts = datetime.datetime.strptime(date, '%Y-%m-%d')
        start_date_ts = start_date_time_ts.strftime("%Y-%m-%d")

        if start_time_ts:
            start_date_time_ts = datetime.datetime.strptime('%s %s' % (start_date_ts, start_time_ts),
                                                            '%Y-%m-%d %H:%M:%S')
        start_time_ts = start_date_time_ts.strftime("%H:%M:%S")

        print 'Start CSVTODSS.py on ', date, '@', time, tag, hec_hms_model_dir
        print ' With Custom starting', start_date_ts, '@', start_time_ts

        my_dss = HecDss.open(dss_input_file)

        file_name = rain_csv_file.rsplit('.', 1)
        # str .format not working on this version
        file_name = '%s-%s%s.%s' % (file_name[0], date, '.' + tag if tag else '', file_name[1])
        rain_csv_file_path = os.path.join(output_dir, file_name)
        print 'Open Rainfall CSV ::', rain_csv_file_path
        csvReader = csv.reader(open(rain_csv_file_path, 'r'), delimiter=',', quotechar='|')
        csvList = list(csvReader)

        numLocations = len(csvList[0]) - 1
        numValues = len(csvList) - num_metadata_lines  # Ignore Metadata
        locationIds = csvList[1][1:]
        print 'Start reading', numLocations, csvList[0][0], ':', ', '.join(csvList[0][1:])
        print 'Period of ', numValues, 'values'
        print 'Location Ids :', locationIds

        for i in range(0, numLocations):
            print '\n>>>>>>> Start processing ', locationIds[i], '<<<<<<<<<<<<'
            precipitations = []
            for j in range(num_metadata_lines, numValues + num_metadata_lines):
                p = float(csvList[j][i + 1])
                precipitations.append(p)

            print 'Precipitation of ', locationIds[i], precipitations[:10]
            tsc = TimeSeriesContainer()
            # tsc.fullName = "/BASIN/LOC/FLOW//1HOUR/OBS/"
            # tsc.fullName = '//' + locationIds[i].upper() + '/PRECIP-INC//1DAY/GAGE/'
            tsc.fullName = '//' + locationIds[i].upper() + '/PRECIP-INC//1HOUR/GAGE/'

            print 'Start time : ', csvList[num_metadata_lines][0]
            start = HecTime(csvList[num_metadata_lines][0])
            tsc.interval = 60  # in minutes
            times = []
            for value in precipitations:
                times.append(start.value())
                start.add(tsc.interval)
            tsc.times = times
            tsc.values = precipitations
            tsc.numberValues = len(precipitations)
            tsc.units = "MM"
            tsc.type = "PER-CUM"
            my_dss.put(tsc)

    except Exception, e:
        MessageBox.showError(' '.join(e.args), "Python Error")
    except java.lang.Exception, e:
        MessageBox.showError(e.getMessage(), "Error")
finally:
    my_dss.done()
    print '\nCompleted converting ', rain_csv_file_path, ' to ', dss_input_file
    print 'done'