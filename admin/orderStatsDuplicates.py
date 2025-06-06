def getSocDB():
	if system.tag.readBlocking('[System]Gateway/SystemName')[0].value == 'Ignition-BLM-SQL02':
		socDB = "glass_cnfsql04"
	else:
		socDB = "glass"
	return socDB


def get_stats_duplicates(sourceID, plantID):
	qry = """
  	SELECT COUNT(orderRowID) as orderRowID_count
  		,orderRowID
		,sourceID
		,plantID
		,lineNumber
		,orderNumber
	FROM soc.[orderStats]
 	WHERE sourceID = {sourceID} AND plantID = {plantID}
	GROUP BY orderRowID, sourceID ,plantID, lineNumber ,orderNumber
		HAVING COUNT(orderRowID) > 1
		order by sourceID ,plantID, lineNumber ,orderNumber
	""".format(sourceID=sourceID, plantID=plantID)
	database = getSocDB()
	pyds = system.db.runQuery(qry,database)
	return pyds


def get_orderTracking_by_orderNumber(sourceID, orderNumber):
	qry = """
  	SELECT * FROM soc.[orderTracking]
 	WHERE sourceID = {sourceID} AND orderNumber = {orderNumber}
	ORDER BY 1 	
	""".format(sourceID=sourceID,orderNumber=orderNumber)
	database = getSocDB()
	pyds = system.db.runQuery(qry,database)
	return pyds


def get_orderTracking_by_recordID(recordID):
	qry = """
  	SELECT * FROM soc.[orderTracking]
 	WHERE order_idx = {recordID}
	ORDER BY 1 	
	""".format(recordID=recordID)
	database = getSocDB()
	pyds = system.db.runQuery(qry,database)
	return pyds


def get_orderStats_by_orderNumber(sourceID, orderNumber):
	qry = """
  	SELECT * FROM soc.[orderStats]
 	WHERE sourceID = {sourceID} AND orderNumber = {orderNumber}
	ORDER BY 1 	
	""".format(sourceID=sourceID,orderNumber=orderNumber)
	database = getSocDB()
	pyds = system.db.runQuery(qry,database)
	return pyds


def get_orderStats_by_recordID(recordID):
	qry = """
  	SELECT * FROM soc.[orderStats]
 	WHERE orderRowID = {recordID}
 	ORDER BY 1
	""".format(recordID=recordID)
	database = getSocDB()
	pyds = system.db.runQuery(qry,database)
	return pyds


def get_order_paths(sourceID,plantID):
	if soc.config.systemName == "Ignition-BLM-SQL02":
		localProviderColumn = "remote"
	else:
		localProviderColumn = "local"
	qry = """
	SELECT sourceID
			,plantID
			,lineLinkID
			,lineNumber
			,CASE
				WHEN '{providerSource}' = 'local'
					THEN CONCAT( '[', providerLocal, ']', orderPath)
				ELSE CONCAT( '[', providerRemote, ']', orderPath)
			END AS orderPath
	FROM soc.plantDef
	WHERE sourceID = {sourceID} AND  plantID = {plantID}
	ORDER BY lineNumber
	""".format(providerSource=localProviderColumn,sourceID=sourceID,plantID=plantID)
	database = getSocDB()
	pyds = system.db.runQuery(qry,database)
	return system.dataset.toDataSet(pyds)


def get_tracking_duplicates(sourceID,plantID):
	qry = """
	SELECT COUNT(orderNumber) AS eventCount, orderNumber
	FROM soc.[orderTracking]
	 	WHERE sourceID = {sourceID} AND  plantID = {plantID} AND orderNumber > 0
	 	--WHERE sourceID = 1 AND plantID = 1 AND orderNumber = 467525
	GROUP BY orderNumber
		HAVING COUNT(orderNumber) > 1
		ORDER BY 1 DESC
	""".format(sourceID=sourceID,plantID=plantID)
	database = getSocDB()
	pyds = system.db.runQuery(qry,database)
	return system.dataset.toDataSet(pyds)


def btn_getDuplicates(event):
	rc = event.source.parent
	sourceID = rc.getComponent("val_sourceID").intValue
	plantID = rc.getComponent("val_plantID").intValue
	rc.getComponent("tbl_stats_duplicates").data = get_stats_duplicates(sourceID,plantID)
	rc.getComponent("tbl_tracking_duplicates").data = get_tracking_duplicates(sourceID,plantID)
	rc.orderPaths = get_order_paths(sourceID,plantID)
	return


def get_historian_data(orderNumber,tagpath,startDate,endDate):
	print 'get_historian_data ',orderNumber,tagpath,startDate,endDate
	ds = system.tag.queryTagHistory([ tagpath ]
									,startDate=startDate
									,endDate=endDate
									,noInterpolation=1
									,returnSize=-1
									,returnFormat="Wide"
									)
	return ds


def get_historian_times(ds, orderNumber):
	print 'get_historian_times '
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


def getSummary(event):

	def get_idx_best_match(compare_value, values):
		min_value = min(map(abs, values))
		for i, value in enumerate(values):
			if value == min_value:
				return i





		return


	# 459796 L10
	rc = event.source.parent
	orderHistory = rc.orderDatesHistorian
	tracking = rc.getComponent("tbl_tracking").data
	matching_records = []
	matching_record_ids = [ ]
	for idx in range(orderHistory.rowCount):
		orderNumber = orderHistory.getValueAt(idx, "orderNumber")
		startTime_h = orderHistory.getValueAt(idx, "startTime")
		endTime_h = orderHistory.getValueAt(idx, "endTime")
		start_time_difference = [ system.date.secondsBetween(startTime_h, row["orderStart"]) for row in system.dataset.toPyDataSet(tracking) ]
		end_time_difference = [ system.date.secondsBetween(endTime_h, row[ "orderEnd" ]) for row in system.dataset.toPyDataSet(tracking) ]

		print idx, start_time_difference, min(map(abs,start_time_difference))

		newList = list(map(abs,start_time_difference))
		print newList


		tracking_idx_best_match = list(map(abs,start_time_difference)).index((min(map(abs,start_time_difference))))
		matching_record_ids.append(tracking.getValueAt(tracking_idx_best_match, "order_idx"))
		matching_records.append([ 	tracking.getValueAt(tracking_idx_best_match, "order_idx")
									,orderNumber
									,startTime_h
									,endTime_h
								  	,start_time_difference[tracking_idx_best_match]
								  	,end_time_difference[tracking_idx_best_match]
								  ])
	# Define invalid order tracking records and craft delete queries
	bad_tracking_records = set( row["order_idx"] for row in system.dataset.toPyDataSet(tracking) ) - set(matching_record_ids)
	missing_tracking_records = set(matching_record_ids) - set( row["order_idx"] for row in system.dataset.toPyDataSet(tracking) )
	insert_queries_tracking = [ ]
	for missing_tracking_record in missing_tracking_records:
		qry = """
		INSERT INTO [soc].[orderTracking]
           ([sourceID]
           ,[plantID]
           ,[lineLinkID]
           ,[lineNumber]
           ,[orderNumber]
           ,[orderStart]
           ,[orderEnd]
           ,[socIn]
           ,[socRevIn]
           ,[socOut]
           ,[socRevOut]
           ,[socFinal]
           ,[socRevFinal]
           ,[lastUpdate]
           ,[lastActor]
           ,[indexStart]
           ,[indexEnd]
           ,[rawOrder]
           ,[quality]
           ,[productCode]
           ,[itemCode]
           ,[targetWidth]
           ,[targetMil]
           ,[validOrder]
           ,[t_stamp]
           ,[erpPlantID]
           ,[erpLineLinkID]
           ,[erpLineNumber])
     VALUES
           (<sourceID, int,>
           ,<plantID, int,>
           ,<lineLinkID, int,>
           ,<lineNumber, int,>
           ,<orderNumber, int,>
           ,<orderStart, datetime,>
           ,<orderEnd, datetime,>
           ,<socIn, int,>
           ,<socRevIn, int,>
           ,<socOut, int,>
           ,<socRevOut, int,>
           ,<socFinal, int,>
           ,<socRevFinal, int,>
           ,<lastUpdate, datetime,>
           ,<lastActor, nvarchar(50),>
           ,<indexStart, datetime,>
           ,<indexEnd, datetime,>
           ,<rawOrder, nvarchar(50),>
           ,<quality, varchar(255),>
           ,<productCode, nvarchar(50),>
           ,<itemCode, nvarchar(50),>
           ,<targetWidth, nvarchar(10),>
           ,<targetMil, nvarchar(10),>
           ,<validOrder, bit,>
           ,<t_stamp, datetime,>
           ,<erpPlantID, int,>
           ,<erpLineLinkID, int,>
           ,<erpLineNumber, int,>)
		"""
		print qry
		insert_queries_tracking.append( qry )


	delete_queries_tracking = []
	# qry_all = "DELETE FROM dbo.orderTracking WHERE order_idx IN ({})".format(",".join(map(str,bad_tracking_records)))
	# print qry_all
	for bad_tracking_record in bad_tracking_records:
		qry = "DELETE FROM dbo.orderTracking WHERE order_idx = {}".format(bad_tracking_record)
		print qry
		delete_queries_tracking.append( qry )
	# Determine if records start/end times are within grace period
	grace_period_seconds = 60
	update_queries_tracking = [ ]
	for record in matching_records:
		if abs(record[4]) > grace_period_seconds or abs(record[5]) > grace_period_seconds:
			print "record out of tolerance ", record

			kwargs = {	"orderStart": system.date.format(record[2], "YYYY-MM-DD HH.mm.ss")
						,"orderEnd": system.date.format(record[3],"YYYY-MM-DD HH.mm.ss")
						,"recordID": record[0]
						,"updated": 1
					  }
			qry = ("UPDATE dbo.orderTracking " 
					"SET orderStart = '{orderStart}' "
					",orderEnd = '{orderEnd}' "
					",updated = 1 "
					"WHERE order_idx = {recordID}").format(**kwargs)
			print qry
			update_queries_tracking.append(qry)

	return


def btn_getHistorianDates(event):
	rc = event.source.parent
	tracking = rc.getComponent("tbl_tracking_selected").data
	if tracking.rowCount > 0:
		sourceID = tracking.getValueAt(0,"sourceID")
		lineNumber = tracking.getValueAt(0,"lineNumber")
		orderNumber = tracking.getValueAt(0,"orderNumber")
		startDate = tracking.getValueAt(0,"orderStart")
		endDate = tracking.getValueAt(0,"orderEnd")
		windowHours = 200
		windowStart = system.date.addHours(startDate, -windowHours)
		windowEnd = system.date.addHours(endDate, windowHours)
		for i in range(rc.orderPaths.rowCount):
			if rc.orderPaths.getValueAt(i,"lineNumber") == lineNumber:
				tagpath = rc.orderPaths.getValueAt(i, "orderPath")
				dsHistorian = get_historian_data(orderNumber,tagpath,windowStart,windowEnd)
				print tagpath, dsHistorian
				rc.getComponent("tbl_historian").data = dsHistorian
				rc.orderDatesHistorian = get_historian_times(dsHistorian, orderNumber)
		getSummary(event)
	return


def val_recordID_propertyChange(event):
	if event.propertyName == "intValue":
		rc = event.source.parent
		recordID = event.newValue
		print 'val_recordID_propertyChange: recordID', recordID
		tracking = get_orderTracking_by_recordID(recordID)
		stats = get_orderStats_by_recordID(recordID)
		print "tracking", tracking
		print "stats",stats
		tbl_tracking = rc.getComponent("tbl_tracking_selected")
		tbl_tracking.data = system.dataset.toDataSet(tracking)
		tbl_stats = rc.getComponent("tbl_stats_selected")
		tbl_stats.data = system.dataset.toDataSet(stats)
		# Selected Row Data
		table = rc.getComponent("tbl_stats_duplicates")
		selectedRow = table.selectedRow
		sourceID = table.data.getValueAt(selectedRow,"sourceID")
		orderNumber = table.data.getValueAt(selectedRow,"orderNumber")
		rc.getComponent("tbl_tracking").data = get_orderTracking_by_orderNumber(sourceID,orderNumber)
		rc.getComponent("tbl_stats").data = get_orderStats_by_orderNumber(sourceID,orderNumber)
	return
