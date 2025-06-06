def get_order_paths(sourceID,plantID,historian):
	qry = """
	SELECT sourceID
			,plantID
			,lineLinkID
			,lineNumber
			,CONCAT( '[', '{historian}', ']', orderPath) AS orderPath
	FROM soc.plantDef
	WHERE sourceID = {sourceID} AND  plantID = {plantID} 
	""".format(historian=historian,sourceID=sourceID,plantID=plantID)
	database = "glass"
	pyds = system.db.runQuery(qry,database)
	order_paths = { row[ "lineNumber" ]: row[ "orderPath" ] for row in pyds }
	print order_paths
	return order_paths


def get_order_dates_historian(orderNumber,tagpath,startDate,endDate):
	print 'get_order_dates_historian ',orderNumber,tagpath,startDate,endDate

	ds = system.tag.queryTagHistory([ tagpath ],startDate=startDate,endDate=endDate,noInterpolation=1)
	newData = [ ]
	cnt = 0
	for row in range(ds.getRowCount()):
		if row != 0:
			if ds.getValueAt(row,1) != lastValue and ds.getValueAt(row,1) != '===============':
				newData.append([ str(ds.getValueAt(row,1)),ds.getValueAt(row,0),ds.getValueAt(row,0) ])
				newData[ cnt ][ 2 ] = ds.getValueAt(row,0)
				cnt = cnt + 1
				lastRow = row
		else:
			if ds.getValueAt(row,1) != '===============':
				newData.append([ str(ds.getValueAt(row,1)),ds.getValueAt(row,0),ds.getValueAt(row,0) ])
				lastRow = row
		lastValue = ds.getValueAt(row,1)
	finalData = [ ]
	for row in newData:
		if str(orderNumber) in row[ 0 ]:
			finalData.append(row)
	headers = [ 'orderNumber','startTime','endTime' ]
	newDS = system.dataset.toDataSet(headers,finalData)
	return newDS


def get_tracking_duplicates():
	qry = """
	SELECT COUNT(orderNumber) AS eventCount, orderNumber
	FROM soc.[orderTracking]
	 	WHERE sourceID = 1 AND plantID = 1 AND orderNumber > 0
	 	--WHERE sourceID = 1 AND plantID = 1 AND orderNumber = 467525
	GROUP BY orderNumber
		HAVING COUNT(orderNumber) > 1
		ORDER BY 1 DESC
	"""
	database = "glass"
	pyds = system.db.runQuery(qry,database)
	return pyds


def get_orderTracking(orderNumber):
	qry = """
	SELECT *
	FROM soc.[orderTracking]
 	WHERE sourceID = 1 AND orderNumber = {orderNumber}
	""".format(orderNumber=orderNumber)
	database = "glass"
	pyds = system.db.runQuery(qry,database)
	return pyds


def process_orderTracking_record(orderTracking,orderTracking_id):
	orderNumer = 123456
	startDate = system.date.parse("05-01-2025 1:00:00","MM/dd/yy HH:mm:ss")
	endDate = system.date.parse("05-12-2025 1:00:00","MM/dd/yy HH:mm:ss")
	windowHours = 4
	windowStart = system.date.addHours(startDate,- windowHours)
	windowEnd = system.date.addHours(endDate,windowHours)
	return


def eval_dates(dsTracking):
	finished_orders = [ row for row in dsTracking if row[ "orderEnd" ] is not None ]
	print "dsTracking rows: ",dsTracking.rowCount,finished_orders
	return


def process_orderTracking_records(sourceID,plantID):
	windowHours = 4
	historian = "Superior"
	order_paths = get_order_paths(sourceID,plantID,historian)
	dsDuplicates = get_tracking_duplicates()
	print
	'dsDuplicates',dsDuplicates
	# oee.util.ptc(dsDuplicates)
	more_than_one_run = [ ]
	for idx in range(dsDuplicates.getRowCount()):
		orderNumber = dsDuplicates.getValueAt(idx,"orderNumber")
		dsTracking = get_orderTracking(orderNumber)
		print 'dsTracking',dsTracking
		# oee.util.ptc(dsTracking)
		finished_orders = [ row for row in dsTracking if row[ "orderEnd" ] is not None ]
		print "dsTracking rows: ",dsTracking.rowCount,len(finished_orders)
		if dsTracking.rowCount != len(finished_orders):
			if len(finished_orders) == 1:
				lineNumber = dsTracking.getValueAt(0,"lineNumber")
				startDate = dsTracking.getValueAt(0,"orderStart")
				endDate = finished_orders[ 0 ][ "orderEnd" ]
				print 'startDate',startDate
				print 'endDate',endDate
				# get_end_dates(dsTracking)
				windowStart = system.date.addHours(startDate,- windowHours)
				windowEnd = system.date.addHours(endDate,windowHours)
				dsOrderDates = get_order_dates_historian(orderNumber,order_paths[ lineNumber ],windowStart,windowEnd)
				print 'dsOrderDates',dsOrderDates
				oee.util.ptc(dsOrderDates)
			else:
				more_than_one_run.append(orderNumber)
		else:
			print "Skipping Order: ",orderNumber
		if idx > 1:
			break
	print 'more_than_one_run',more_than_one_run
	return







