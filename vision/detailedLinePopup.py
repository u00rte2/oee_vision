def visionWindowOpened(event):
    self = event.source.parent
    window = system.gui.getParentWindow(event)
    rc = window.getRootContainer()
    header = rc.getComponent("Header")
    info = rc.getComponent("Info")

    print("rc ", type(rc), rc)
    print("header ", type(header), header)
    print("info ", type(info), info)

    log = system.util.getLogger('OEE EVENT')
    log.info('START')

    rc.getComponent("Loading").visible = True
    info.visible = False
    header.getComponent("OEE").visible = False

    StartTime = rc.startTime
    EndTime = rc.endTime
    startTimeRange = system.date.addWeeks(StartTime, -4)
    endTimeRange = system.date.addWeeks(EndTime, 1)


    rc.mode = 'historic'
    header.getComponent("Date Range").outerRangeStartDate = startTimeRange
    header.getComponent("Date Range").outerRangeEndDate = endTimeRange
    header.getComponent("Date Range").startDate = StartTime
    header.getComponent("Date Range").endDate = EndTime

    LocationName = rc.plantName + '/Line' + str(rc.lineNumber)


    changeoverData = GMS.OEE.getChangeover(StartTime, EndTime, LocationName)

    downData = info.getComponent("Template Repeater").templateParams


    updates = {"EndDate": EndTime, "FQPath": LocationName, "StartDate": StartTime, "refresh": True}
    newDownDS = system.dataset.updateRow(downData, 0, updates)

    avail = GMS.OEE.getAvailability(StartTime, EndTime, LocationName)
    perf = GMS.OEE.getPerformance(StartTime, EndTime, LocationName)
    qual = GMS.OEE.getQuality(StartTime, EndTime, LocationName)

    header.getComponent("OEE").availabilityOEE = avail
    header.getComponent("OEE").performanceOEE = perf
    header.getComponent("OEE").qualityOEE = qual

    info.getComponent("Template Repeater").templateParams = newDownDS
    info.getComponent("AverageChangeoverTable").data = changeoverData


    liveData = GMS.OEE.getRateTime(StartTime, EndTime, rc.plantName, rc.lineNumber)



    chart = info.getComponent("RateChart")
    print("chart ", type(chart), chart)

    ds = info.getComponent("RateChart").getPropertyValue("Data")
    print('ds ', ds)

    info.getComponent("RateChart").setPropertyValue( "Data", liveData )






    livePerformance = GMS.OEE.getLivePerformance(StartTime, EndTime, liveData, LocationName)
    header.getComponent("OEE").liveOEE = livePerformance['performance']



    # Calculate Performance Data
    params = {'StartTime': StartTime, 'EndTime': EndTime, 'LocationName': LocationName}
    rawPerformanceData = system.db.runNamedQuery('GMS/OEE/GetHistoricPerformance', params)

    aggregatePerformanceHeaders = ["WorkOrderUUID", "LocationName", "ActualStartTime", "ActualEndTime", "OutfeedCount",
                                   "RejectCount", "StandardRate", "Timestamp", "Performance"]
    aggregatePerformanceData = []
    lastWorkOrder = None
    lastLocationName = None
    rowStartTime = None
    rowEndTime = None
    rowOutfeed = 0
    rowReject = 0
    rowStandardRate = 0
    rowTimeStamp = None
    for row in rawPerformanceData:

        if (row['WorkOrderUUID'] != lastWorkOrder and lastWorkOrder != None) or (
                row['LocationName'] != lastLocationName and lastLocationName != None):
            # Calcualte Aggregate Performance
            Performance = ((float(rowOutfeed - rowReject) / (
                        float(system.date.secondsBetween(rowStartTime, rowEndTime)) / 3600)) / rowStandardRate) * 100.0

            # Create Row for Table
            aggregateRow = [lastWorkOrder, lastLocationName, rowStartTime, rowEndTime, rowOutfeed, rowReject,
                            rowStandardRate, rowTimeStamp, Performance]
            aggregatePerformanceData.append(aggregateRow)

            # Reset Values
            rowStartTime = None
            rowEndTime = None
            rowOutfeed = 0
            rowReject = 0
            rowStandardRate = 0
            rowTimeStamp = None

        # Keep track of last Work Order and Location Name
        lastWorkOrder = row['WorkOrderUUID']
        lastLocationName = row['LocationName']

        # Get the earliest Start Time
        if rowStartTime == None or row['ActualStartTime'] < rowStartTime:
            rowStartTime = row['ActualStartTime']

        # Get the latest End Time
        if rowEndTime == None or row['ActualEndTime'] > rowEndTime:
            rowEndTime = row['ActualEndTime']

        # Get sum of Outfeed Count and Reject Count
        rowOutfeed += row['OutfeedCount']
        rowReject += row['RejectCount']

        # Get the latest Standard Rate and timestamp
        rowStandardRate = row['StandardRate']
        rowTimeStamp = row['Timestamp']

    # Calcualte Last Row Aggregate Performance
    Performance = ((float(rowOutfeed - rowReject) / (
                float(system.date.secondsBetween(rowStartTime, rowEndTime)) / 3600)) / rowStandardRate) * 100.0

    # Create Last Row for Table
    row = [lastWorkOrder, lastLocationName, rowStartTime, rowEndTime, rowOutfeed, rowReject, rowStandardRate,
           rowTimeStamp, Performance]
    aggregatePerformanceData.append(row)

    # Create performanceData
    performanceData = system.dataset.toDataSet(aggregatePerformanceHeaders, aggregatePerformanceData[1:])
    info.getComponent("PerformanceTable").data = performanceData

    qualityData = system.db.runNamedQuery('GMS/OEE/GetHistoricQuality', params)
    info.getComponent("RejectTable").data = qualityData

    rc.getComponent("Loading").visible = False
    info.visible = True
    header.getComponent("OEE").visible = True



    log.info('END')

    return


def convert_millis(millis):
    minutes, seconds = divmod(millis/1000, 60)
    hours, minutes = divmod(minutes, 60)
    return "%02d:%02d:%02d" % (hours, minutes, seconds)


def getChangeover():
    """
    Changeover data and aggregation of given LocationName between StartTime and EndTime
    Returns dataset including level, total, and average of each level of changeover
    """
    headers = ['Level', 'Total', 'Average']
    finalData = []
    level = 1
    now = system.date.now()
    loc = oee.vision.main.read_location()
    print(loc)
    line_downtime = system.tag.readBlocking( ["[client]oee/line/downtimeEvents"] )[0].value
    # Iterate through each event code for changeover levels 1 - 6
    for eventCode in range(205, 211):
        # Get changeover level data
        f_columns = ["EventCode"]
        f_values = [eventCode]
        dataset = oee.util.filterDataset(line_downtime, f_columns, f_values)
        if dataset.rowCount > 0:
            totalMillis = 0
            for i in range(dataset.rowCount):
                evt_start = dataset.getValueAt(i, 'StartTime')
                evt_end = dataset.getValueAt(i, 'EndTime')
                end = dataset.getValueAt(i, 'EndTime') if evt_end is not None else now
                millis = system.date.millisBetween(evt_start, end)
                totalMillis += millis
            totalStr = convert_millis(totalMillis)
            avgStr = convert_millis(int(totalMillis / dataset.rowCount))
            title = 'Level ' + str(level)
            finalData.append([title, totalStr, avgStr])
        level = level + 1
    return system.dataset.toDataSet(headers, finalData)


def get_downtime_occurances(event):
    downtimeEvents = system.tag.readBlocking(["[client]oee/plant/downtimeEvents"])[0].value
    rc = system.gui.getParentWindow(event).getRootContainer()
    info = rc.getComponent('Info')
    stateList = []
    occDict = {}
    durDict = {}
    # Find every instance of Unplanned Downtime
    dt_events = oee.util.filterDataset(downtimeEvents, ["IsDowntime"], [1])
    print(dt_events)
    for row in range(downtimeEvents.getRowCount()):
        if downtimeEvents.getValueAt(row, 'IsDowntime') == 1:  # and data.getValueAt(row, 'IsPlanned') == 0:
            # Read values and calculate duration
            stateName = str(downtimeEvents.getValueAt(row, 'Name'))
            startTime = downtimeEvents.getValueAt(row, 'StartTime')
            endTime = downtimeEvents.getValueAt(row, 'EndTime')
            if endTime is None:
                endTime = system.date.now()
            duration = system.date.minutesBetween(startTime, endTime)
            # If this is a new state name, add it to the list and set initial dictionary values.
            if stateName not in stateList:
                stateList.append(stateName)
                occDict[stateName] = 1
                durDict[stateName] = duration
            # If this is a repeat state name, increment dictionary values by the appropriate amount.
            else:
                occDict[stateName] += 1
                durDict[stateName] += duration
    # Convert Dictionaries to Datasets
    occHeaders = ['State', 'Occurances']
    occData = []
    durHeaders = ['State', 'Duration']
    durData = []
    sumHeaders = ['State', 'Occurances', 'Duration Minutes']
    sumData = []
    stateList.sort()
    for stateName in stateList:
        occData.append([stateName, occDict[stateName]])
        durData.append([stateName, durDict[stateName]])
        sumData.append([stateName, occDict[stateName], durDict[stateName]])
    occuranceData = system.dataset.toDataSet(occHeaders, occData)
    durationData = system.dataset.toDataSet(durHeaders, durData)
    summaryData = system.dataset.toDataSet(sumHeaders, sumData)
    info.getComponent('Actual Scrap Chart').AOccurance = occuranceData
    info.getComponent('Actual Scrap Chart').BDuration = durationData
    info.getComponent('tbl_summary').data = summaryData
    return