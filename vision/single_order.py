def getAdjacentOrderNumbers(database,kwargs):

	qry = """
	SELECT TOP (1000) o.[Orders_idx]
		  ,o.[sourceID]
		  ,o.[plantID]
		  ,o.[lineNumber]
		  ,(SELECT TOP(1) [orderNumber] FROM soc.Orders WHERE sourceID = {sourceID} AND plantID = {plantID} AND lineNumber = {lineNumber} AND orderStart < o.orderStart  ORDER BY orderStart DESC ) AS previousOrder
		  ,(SELECT TOP(1) orderStart FROM soc.Orders WHERE sourceID = {sourceID} AND plantID = {plantID} AND lineNumber = {lineNumber} AND orderStart < o.orderStart  ORDER BY orderStart DESC) AS previousOrderStart
		  ,(SELECT TOP(1) orderEnd FROM soc.Orders WHERE sourceID = {sourceID} AND plantID = {plantID} AND lineNumber = {lineNumber} AND orderStart < o.orderStart  ORDER BY orderStart DESC) AS previousOrderEnd
		  ,o.[orderNumber]
		  ,o.[orderStart]
		  ,o.[orderEnd]
		  ,(SELECT TOP(1) [orderNumber] FROM soc.Orders WHERE sourceID = {sourceID} AND plantID = {plantID} AND lineNumber = {lineNumber} AND orderStart > o.orderStart  ORDER BY orderStart ) AS nextOrder
		  ,(SELECT TOP(1) orderStart FROM soc.Orders WHERE sourceID = {sourceID} AND plantID = {plantID} AND lineNumber = {lineNumber} AND orderStart > o.orderStart  ORDER BY orderStart ) AS nextOrderStart
		  ,(SELECT TOP(1) orderEnd FROM soc.Orders WHERE sourceID = {sourceID} AND plantID = {plantID} AND lineNumber = {lineNumber} AND orderStart > o.orderStart  ORDER BY orderStart ) AS nextOrderEnd
	  FROM [Glass].[soc].[Orders] o
	WHERE sourceID = {sourceID}
		AND plantID = {plantID}
		AND lineNumber = {lineNumber}
		AND orderNumber = {orderNumber}
		""".format(**kwargs)
	return system.db.runQuery(qry,database)


def refresh_data(sourceID,plantID,lineNumber,orderNumber):
	kwargs = {
		"sourceID": sourceID,
		"plantID": plantID,
		"lineNumber": lineNumber,
		"orderNumber": orderNumber
	}
	glassDB = oee.util.get_glass_db()
	erpDB = "CharterSQL_RC"
	pyOrders = getAdjacentOrderNumbers(glassDB,kwargs)
	r = system.tag.writeBlocking(["[client]oee/singleOrder/orderNumbers"],[system.dataset.toDataSet(pyOrders)])
	orders = []
	for row in pyOrders:
		for col in ("previousOrder","orderNumber","nextOrder"):
			if row[col] != 0 and row[col] not in orders :
				orders.append(row[col])
	orderNumbers = tuple(orders)  #( pyOrders[0]["previousOrder"], pyOrders[0]["orderNumber"], pyOrders[0]["nextOrder"] )
	dsMap = oee.vision.equipmentSchedule.get_dsMap()
	dsMap["downtimeEvents"] = {"dsName": "downtimeEvents", "id": "ID", "startDate": "StartTime", "endDate": "EndTime", "label": "orderNumber", "orderNumber": "WorkOrderUUID"}
	dsNames = [dsMap[key]["dsName"] for key in dsMap.keys()]
	datasets = {
				"orderTracking": oee.roll_detail.getOrderTracking(glassDB, sourceID, orderNumbers)
				,"orderStats": oee.roll_detail.getOrderStats(glassDB, sourceID, orderNumbers)
				,"erpData": oee.roll_detail.getErpProductionData(erpDB, orderNumbers)
				,"erpRolls": oee.roll_detail.getErpProductionItems(erpDB, orderNumbers)
				,"downtimeEvents": oee.roll_detail.getDowntimeEvents(glassDB,sourceID, orderNumbers)
				,"machine_orders": oee.roll_detail.get_machine_orders(glassDB, orderNumbers)
				,"machine_roll_detail": oee.roll_detail.get_machine_roll_detail(glassDB, orderNumbers)
				}
	tagpaths = { "[client]oee/singleOrder/{}".format(dsName): system.dataset.toDataSet(datasets[dsName]) for dsName in dsNames }
	r = system.tag.writeBlocking(tagpaths.keys(), tagpaths.values())
	startDate = endDate = None
	minDates = []
	maxDates = []
	for dsName, pyds in datasets.iteritems():
		print dsName, pyds, dsMap[dsName]["startDate"]
		if pyds.rowCount > 0:
			minDate = min(filter(None,[row[dsMap[dsName]["startDate"]] for row in pyds]))
			maxDate = max(filter(None,[row[dsMap[dsName]["endDate"]] for row in pyds]))
			if len(minDates) > 0:
				minDates.append(minDate)
			if len(maxDates) > 0:
				maxDates.append(maxDate)
	if len(minDates) > 0:
		startDate = min(filter(None, [date_value for date_value in minDates]))
	if len(maxDates) > 0:
		endDate = min(filter(None, [date_value for date_value in maxDates]))
	oee_plant = oee.vision.main.get_plant_oee_v2(datasets["orderStats"], datasets["erpRolls"], datasets["downtimeEvents"])
	template_data = oee.vision.main.get_template_data(oee_plant, sourceID,plantID,startDate,endDate)
	tagpaths = { "[client]oee/singleOrder/oeeData": oee_plant,"[client]oee/singleOrder/template_data": template_data }
	r = system.tag.writeBlocking(tagpaths.keys(),tagpaths.values())
	oee.vision.equipmentSchedule.getScheduleData(lineNumber, "singleOrder")
	return