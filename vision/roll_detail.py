def getOrdersByDateRange(database, sourceID, plantID, startDate, endDate):
	qry_old = """
	SELECT DISTINCT
		[sourceID]
		,[plantID]
		,[lineLinkID]
		,[lineNumber]
		,[orderNumber]
	FROM soc.orderStats
	WHERE sourceID = ?
		AND plantID = ?
		AND graphStart BETWEEN ? AND ?
		AND graphEnd IS NOT NULL
	"""
	qry = """
	SELECT DISTINCT
		[sourceID]
		,[plantID]
		,[lineNumber]
		,[orderNumber]
	FROM soc.Orders
	WHERE sourceID = ?
		AND plantID = ?
		AND orderStart BETWEEN ? AND ?
	"""
	return system.db.runPrepQuery(qry, [ sourceID, plantID, startDate, endDate ], database)


def getOrderTracking(database, sourceID, orderNumbers):
	qry = """
	SELECT TOP(10000) *
			,CAST(DATEDIFF(second, [orderStart], [orderEnd]) / 3600.0 AS DECIMAL(8,3)) AS duration_hours
	FROM soc.orderTracking
	WHERE sourceID = {sourceID} AND orderNumber IN {orderNumbers}
	ORDER BY sourceID, plantID, lineNumber, orderNumber
	""".format(sourceID=sourceID, orderNumbers=orderNumbers)
	return system.db.runQuery(qry, database)


def getDowntimeEvents(database, sourceID, orderNumbers):
	orderStrings = tuple(str(orderNumber) for orderNumber in orderNumbers)
	qry = """
	SELECT TOP(10000)
		a.*,
		b.ParentEventCode,
		b.[Name], 
		b.[Description], 
		b.IsDowntime, 
		b.IsPlanned,
		DATEDIFF(hour, a.StartTime, a.EndTime) AS 'Hours', 
		DATEDIFF(minute, a.StartTime, a.EndTime) % 60 AS 'Minutes',
		CONVERT(VARCHAR(5), DATEADD(SECOND,DATEDIFF(SECOND, a.StartTime, a.EndTime), 0), 108) AS 'hh:mm',
		CASE
			WHEN b.ParentEventCode = 0 THEN 'Running'
			WHEN b.ParentEventCode = 2 THEN 'Planned Downtime'
			WHEN b.ParentEventCode = 3 THEN 'Unplanned Downtime'
			ELSE 'Error in parent code'
		END AS [State],
		b.[color] AS [Color]	
	FROM soc.DowntimeEvents a
	JOIN soc.DowntimeCodes b
		ON a.EventCode = b.EventCode
	WHERE sourceID = {sourceID}
		AND WorkOrderUUID IN {orderNumbers}
	ORDER BY StartTime ASC
	""".format(sourceID=sourceID, orderNumbers=orderStrings)
	return system.db.runQuery(qry, database)


def getOrderStats(database, sourceID, orderNumbers):
	qry = """
	SELECT TOP(10000) o.* 
		,CAST(DATEDIFF(second, o.[graphStart], o.[graphEnd]) / 3600.0 AS DECIMAL(8,3)) AS duration_hours
		,p.plantName
	FROM soc.orderStats o
	LEFT JOIN soc.plantDef p WITH (NOLOCK) ON o.sourceID = p.sourceID AND o.plantID = p.plantID AND o.lineNumber = p.lineNumber
	WHERE o.sourceID = {sourceID} AND o.orderNumber IN {orderNumbers}
	ORDER BY o.sourceID, o.plantID, o.lineNumber, o.orderNumber
	""".format(sourceID=sourceID, orderNumbers=orderNumbers)
	return system.db.runQuery(qry, database)


def getErpProductionData(database, orderNumbers):
	qry = """
	SELECT TOP (1000)
			1 AS sourceID
			,SalesOrders.plantRoutingID AS plantID
			,COALESCE(schedulemaster.linenumber, ProductionLineInfo.lineInfoID) AS lineLinkID
			,COALESCE(ProductionLineInfo.linenumber, schline.linenumber) AS lineNumber
			,SalesOrders.ordernumber AS orderNumber_sales
			,ProductionData.orderNumber
			,SalesOrders.qty AS orderQuantity
			,ScheduleMASTER.orderBalance
			,SalesOrders.jobStatus
			,ProductionData.prodTargetRollWeight AS targetRollWeight
			,ProductionData.prodTargetRollFootage AS targetRollLength
			,WorkOrder.runRolls
			,WorkOrder.rollsAcross
			,ProductionData.prodStartDate
			,ProductionData.prodCompleteDate
			,SalesOrders.itemMasterID
			,ItemMaster.itemMasterNumber AS itemCode
			,SalesOrders.cnumber AS productCode
			,SalesOrders.WidthInInches AS targetWidth
			,SalesOrders.mil AS targetMil
			,schedulemaster.lbsHour AS erpTarget
			,ProductionLineInfo.targetRate AS defaultTarget
			,ProductionData.prodComplete
			,ProductionData.prodStartBalance
			,ProductionData.prodStartFootage
			,ProductionData.prodStartQty
			,ProductionData.prodTotalQty
			,ProductionData.FeetPerPound
			,CAST(DATEDIFF(second, ProductionData.prodStartDate, ProductionData.prodCompleteDate) / 3600.0 AS DECIMAL(8,3)) AS duration_hours
	FROM [CNFSQLProd01].[CharterSQL_RC].[dbo].[salesorders] WITH (NOLOCK)
			JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].[ItemMaster] WITH (NOLOCK) ON SalesOrders.itemMasterID = ItemMaster.itemMasterID
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].productiondata WITH (NOLOCK) ON SalesOrders.ordernumber = productiondata.ordernumber 
			INNER JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].WorkOrder WITH (NOLOCK) ON SalesOrders.orderNumber = WorkOrder.orderNumber 
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].ProductionLineInfo WITH (NOLOCK) ON ProductionLineInfo.lineInfoID = productiondata.prodLineID
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].schedulemaster WITH (NOLOCK) ON schedulemaster.orderNumber = SalesOrders.orderNumber
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].ProductionLineInfo SchLine WITH (NOLOCK) ON schedulemaster.lineNumber = SchLine.lineInfoID
	WHERE ProductionData.orderNumber IN {orderNumbers}
	ORDER BY SalesOrders.plantRoutingID, COALESCE(ProductionLineInfo.linenumber, schline.linenumber), ProductionData.orderNumber
	""".format(orderNumbers=orderNumbers)
	return system.db.runQuery(qry, database)


def getErpProductionItems(database, orderNumbers):
	qry = """
	SELECT TOP(50000) 
				1 AS sourceID
				,SalesOrders.plantRoutingID AS plantID
				,ProductionLineInfo.lineInfoID AS lineLinkID
				,ProductionLineInfo.lineNumber
				,CAST(DATEDIFF(second, productionItems.pitStartTime, productionItems.pitEndTime) / 3600.0 AS DECIMAL(6,2)) AS duration_hours
				,productionItems.* 
	FROM dbo.productionItems
	JOIN dbo.SalesOrders WITH (NOLOCK)
		ON SalesOrders.ordernumber = productionItems.prodOrderNumber
	LEFT JOIN dbo.ProductionLineInfo WITH (NOLOCK) 
		ON ProductionLineInfo.lineInfoID = productionItems.prodLineID
	WHERE productionItems.orderNumber IN {orderNumbers}
	ORDER BY SalesOrders.plantRoutingID, ProductionLineInfo.lineNumber, productionItems.orderNumber
	""".format(orderNumbers=orderNumbers)
	return system.db.runQuery(qry, database)


def getIndexesByID(database, idNumbers):
	qry = """
	SELECT TOP(50000) * 
		,CAST(DATEDIFF(second, startTime, endTime) / 3600.0 AS DECIMAL(6,2)) AS duration_hours
	FROM dbo.indexTimes
	WHERE indextimes_ndx IN {idNumbers}
	ORDER BY 1
	""".format(idNumbers=idNumbers)
	return system.db.runQuery(qry, database)


def getIndexesByOrderNumber(database, orderNumber):
	qry = """
	SELECT TOP(50000) * 
		,CAST(DATEDIFF(second, startTime, endTime) / 3600.0 AS DECIMAL(6,2)) AS duration_hours
	FROM dbo.indexTimes
	WHERE Order_Number LIKE '%{orderNumber}%'
	ORDER BY 1
	""".format(orderNumber=orderNumber)
	return system.db.runQuery(qry, database)


def get_machine_roll_detail(database, orderNumbers):
	qry = """
	SELECT TOP(50000) * 
		,CAST(DATEDIFF(second, [rollStart], [rollEnd]) / 3600.0 AS DECIMAL(8,3)) AS duration_hours
	FROM soc.rollDetail
	WHERE orderNumber IN {orderNumbers}
	ORDER BY rollStart
	""".format(orderNumbers=orderNumbers)
	return system.db.runQuery(qry, database)


def get_machine_orders(database, orderNumbers):
	qry = """
	SELECT TOP(50000) o.* 
		,CAST(DATEDIFF(second, [orderStart], [orderEnd]) / 3600.0 AS DECIMAL(8,3)) AS duration_hours
		,p.lineLinkID
		,p.plantName
	FROM soc.Orders o
	LEFT JOIN soc.plantDef p WITH (NOLOCK) ON o.sourceID = p.sourceID AND o.plantID = p.plantID AND o.lineNumber = p.lineNumber
	WHERE o.sourceID = 1
		AND o.orderNumber IN {orderNumbers}
	ORDER BY 1
	""".format(orderNumbers=orderNumbers)
	return system.db.runQuery(qry, database)


def getIndexesByWindow(database, windowTimes):
	whereClause = ""
	i = 0
	for lineLinkID, startTime, endTime in windowTimes:
		clause = "lineInfoID={lineLinkID} AND startTime > '{startTime}' AND endTime < '{endTime}'".format(lineLinkID=lineLinkID, startTime=startTime, endTime=endTime)
		if i == 0:
			whereClause += "WHERE ({})".format(clause)
		else:
			whereClause += "\n    OR ({})".format(clause)
		i += 1
	qry = """
	SELECT
	TOP(100) * 
		,CAST(DATEDIFF(second, startTime, endTime) / 3600.0 AS DECIMAL(8,3)) AS duration_hours
	FROM dbo.IndexTimes
	{whereClause}
	ORDER BY 1
	""".format(whereClause=whereClause)
	print qry
	return system.db.runQuery(qry, database)


def getWindowTimes(orderStats):
	windowTimes = []
	for row in orderStats:
		lineLinkID = row["lineLinkID"]
		startTime = system.date.format(system.date.addHours(row["graphStart"],-4), 'MM-dd-yyyy hh:mm:ss a')
		endTime = system.date.format(system.date.addHours(row["graphEnd"],4),'MM-dd-yyyy hh:mm:ss a')
		windowTimes.append( [lineLinkID, startTime, endTime] )
	return windowTimes


def btn_getWindowIndexes(event):
	rc = system.gui.getParentWindow(event).getRootContainer()
	tbl_orderStats = rc.getComponent("cnt_orderStats").getComponent("tbl_data")
	pyOrder = system.dataset.toPyDataSet(tbl_orderStats.data)
	selectedRow = tbl_orderStats.selectedRow
	rowData = pyOrder[selectedRow]
	lineLinkID = rowData[ "lineLinkID" ]
	startTime = system.date.format(system.date.addHours(rowData[ "graphStart" ],-4),'MM-dd-yyyy hh:mm:ss a')
	endTime = system.date.format(system.date.addHours(rowData[ "graphEnd" ],4),'MM-dd-yyyy hh:mm:ss a')
	windowTimes = [ [ lineLinkID, startTime, endTime ] ]
	machineDB = "SuperiorSQL"
	qry = """
	SELECT
	TOP(100) * 
	FROM dbo.IndexTimes
	WHERE lineInfoID={lineLinkID} AND startTime > '{startTime}' AND endTime < '{endTime}'
	ORDER BY 1
	""".format(lineLinkID=lineLinkID,startTime=startTime,endTime=endTime)

	indexesWindow = system.db.runQuery(qry, machineDB)
	rc.getComponent("cnt_indexesWindow").getComponent("tbl_data").data = indexesWindow
	return


def get_roll_detail(event, getAll=False):
	rc = system.gui.getParentWindow(event).getRootContainer()
	glassDB = oee.util.get_glass_db()
	erpDB = "CharterSQL_RC"
	machineDB = "SuperiorSQL"
	if getAll:
		ds_orders = getOrdersByDateRange(glassDB, rc.sourceID, rc.plantID, rc.orderStart, rc.orderEnd)
		orderNumbers = tuple(filter(None,set(row[ "orderNumber" ] for row in ds_orders)))
	else:
		headers = [ "sourceID", "plantID", "lineLinkID", "lineNumber", "orderNumber" ]
		rows = [ [ rc.sourceID, rc.plantID, rc.lineLinkID, rc.lineNumber, rc.orderNumber ] ]
		ds_orders = system.dataset.toDataSet(headers, rows)
		orderNumbers = (rc.orderNumber, rc.orderNumber)
	orderNumber = rc.orderNumber
	orderTracking = getOrderTracking(glassDB, rc.sourceID, orderNumbers)
	orderStats = getOrderStats(glassDB, rc.sourceID, orderNumbers)
	productionData = getErpProductionData(erpDB, orderNumbers)
	productionItems = getErpProductionItems(erpDB, orderNumbers)
	idNumbers = tuple( filter( None, set( row["pitID"] for row in productionItems ) ) )
	if len(idNumbers) == 1:
		idNumbers = (idNumbers[0], idNumbers[0])
	indexesERP = getIndexesByID(machineDB, idNumbers)
	indexesMachine =  getIndexesByOrderNumber(machineDB, orderNumber)
	#oee.util.flipDataset(ds)
	# Update container title with data length
	rc.getComponent("cnt_orderNumbers").getBorder().setTitle( "Order Numbers: {}".format( ds_orders.rowCount ) )
	rc.getComponent("cnt_orderTracking").getBorder().setTitle( "Order Tracking: {}".format( orderTracking.rowCount ) )
	rc.getComponent("cnt_orderStats").getBorder().setTitle( "Order Stats: {}".format( orderStats.rowCount ) )
	rc.getComponent("cnt_productionData").getBorder().setTitle( "Production Data: {}".format( productionData.rowCount ) )
	rc.getComponent("cnt_productionItems").getBorder().setTitle( "Production Items: {}".format( productionItems.rowCount ) )
	rc.getComponent("cnt_indexesERP").getBorder().setTitle( "Indexes ERP: {}".format( indexesERP.rowCount ) )
	rc.getComponent("cnt_indexesMachine").getBorder().setTitle( "Indexes Machine: {}".format( indexesMachine.rowCount ) )
	# Set Tables
	rc.getComponent("cnt_orderNumbers").getComponent("tbl_data").data = ds_orders
	rc.getComponent("cnt_orderTracking").getComponent("tbl_data").data = orderTracking
	rc.getComponent("cnt_orderStats").getComponent("tbl_data").data = orderStats
	rc.getComponent("cnt_productionData").getComponent("tbl_data").data = productionData
	rc.getComponent("cnt_productionItems").getComponent("tbl_data").data = productionItems
	rc.getComponent("cnt_indexesERP").getComponent("tbl_data").data = indexesERP
	rc.getComponent("cnt_indexesMachine").getComponent("tbl_data").data = indexesMachine
	return


def get_roll_detail_all(event):
	rc = system.gui.getParentWindow(event).getRootContainer()
	glassDB = oee.util.get_glass_db()
	erpDB = "CharterSQL_RC"
	machineDB = "SuperiorSQL"
	ds_orders = getOrdersByDateRange(glassDB, rc.sourceID, rc.plantID, rc.orderStart, rc.orderEnd)
	print ds_orders
	orderNumbers = tuple(filter(None,set(row[ "orderNumber" ] for row in ds_orders)))
	orderNumber = rc.orderNumber
	orderTracking = getOrderTracking(glassDB, orderNumbers)
	orderStats = getOrderStats(glassDB, orderNumbers)
	productionData = getErpProductionData(erpDB, orderNumbers)
	productionItems = getErpProductionItems(erpDB, orderNumbers)
	idNumbers = tuple( filter( None, set( row["pitID"] for row in productionItems ) ) )
	indexesERP = getIndexesByID(machineDB, idNumbers)
	indexesMachine =  getIndexesByOrderNumber(machineDB, orderNumber)
	#oee.util.flipDataset(ds)
	# Update container title with data length
	rc.getComponent("cnt_orderNumbers").getBorder().setTitle( "Order Numbers: {}".format( ds_orders.rowCount ) )
	rc.getComponent("cnt_orderTracking").getBorder().setTitle( "Order Tracking: {}".format( orderTracking.rowCount ) )
	rc.getComponent("cnt_orderStats").getBorder().setTitle( "Order Stats: {}".format( orderStats.rowCount ) )
	rc.getComponent("cnt_productionData").getBorder().setTitle( "Production Data: {}".format( productionData.rowCount ) )
	rc.getComponent("cnt_productionItems").getBorder().setTitle( "Production Items: {}".format( productionItems.rowCount ) )
	rc.getComponent("cnt_indexesERP").getBorder().setTitle( "Indexes ERP: {}".format( indexesERP.rowCount ) )
	rc.getComponent("cnt_indexesMachine").getBorder().setTitle( "Indexes Machine: {}".format( indexesMachine.rowCount ) )
	# Set Tables
	rc.getComponent("cnt_orderNumbers").getComponent("tbl_data").data = ds_orders
	rc.getComponent("cnt_orderTracking").getComponent("tbl_data").data = orderTracking
	rc.getComponent("cnt_orderStats").getComponent("tbl_data").data = orderStats
	rc.getComponent("cnt_productionData").getComponent("tbl_data").data = productionData
	rc.getComponent("cnt_productionItems").getComponent("tbl_data").data = productionItems
	rc.getComponent("cnt_indexesERP").getComponent("tbl_data").data = indexesERP
	rc.getComponent("cnt_indexesMachine").getComponent("tbl_data").data = indexesMachine
	return