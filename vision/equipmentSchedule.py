from java.lang import Integer,String,Boolean,Float
from java.util import Date

def visionWindowOpened(event):
	rc = system.gui.getParentWindow(event).getRootContainer()
	getChartData(rc)
	scheduledEvents = rc.getComponent("Equipment Schedule").scheduledEvents
	first_order_number = scheduledEvents.getValueAt(0, "orderNumber")
	filterByOrder(rc, first_order_number)
	return


def get_dsMap():
	dsMap = {
		"orderTracking": {"dsName": "orderTracking", "id": "order_idx", "startDate": "orderStart", "endDate": "orderEnd", "label": "orderNumber", "orderNumber": "orderNumber"}
		#,"orderTracking_index": {"dsName": "orderTracking", "id": "order_idx", "startDate": "indexStart", "endDate": "indexEnd", "label": "orderNumber", "orderNumber": "orderNumber"}
		,"orderStats": {"dsName": "orderStats", "id": "orderStats_ndx", "startDate": "graphStart", "endDate": "graphEnd", "label": "orderNumber", "orderNumber": "orderNumber"}
		,"erpData": {"dsName": "erpData", "id": "idx", "startDate": "prodStartDate", "endDate": "prodCompleteDate", "label": "orderNumber", "orderNumber": "orderNumber"}
		,"erpRolls": {"dsName": "erpRolls", "id": "ID","startDate": "pitStartTime","endDate": "pitEndTime","label": "itemNumber", "orderNumber": "orderNumber"}
		,"machine_orders": {"dsName": "machine_orders","id": "Orders_idx","startDate": "orderStart","endDate": "orderEnd","label": "rawOrder","orderNumber": "orderNumber"}
		,"machine_roll_detail": {"dsName": "machine_roll_detail","id": "rollDetail_idx","startDate": "rollStart","endDate": "rollEnd","label": "rawOrder","orderNumber": "orderNumber"}
	}
	return dsMap


def create_items(lineNumber):
	"""
	Args:
		lineNumber:

	Returns:
		items dataset with entry for each source

	orderTracking: orderStart, orderEnd, indexStart, indexEnd
	orderStats: graphStart, graphEnd
	erpData: prodStartDate, prodCompleteDate
	erpRolls: pitStartTime, pitEndTime
	"""
	ds = system.tag.readBlocking(["[client]oee/plant/template_data"])[0].value
	imagePath = "Builtin/icons/24/media_play.png"
	foreground = system.gui.color(0,0,0,255)
	background = system.gui.color(192,192,192,255)
	colNames = ["ID","Label","StatusImagePath","Foreground","Background","dsName"]
	rows = []
	#for dsName in ["orderTracking","orderTracking_index","orderStats","erpData","erpRolls"]:
	for dsName in ["erpData","erpRolls","orderTracking","orderStats","machine_orders","machine_roll_detail_A","machine_roll_detail_B"]:
		row=[
			"L{}-{}".format(lineNumber, dsName)
			,"L{}-{}".format(lineNumber, dsName)
			,imagePath
			,foreground
			,background
			,dsName
		]
		rows.append(row)
	items = system.dataset.toDataSet(colNames, rows)
	return items


def get_datasets():
	dsMap = get_dsMap()
	dsNames = [ dsMap[key]["dsName"] for key in dsMap.keys() ]
	paths = [ "[client]oee/line/{}".format( dsMap[key]["dsName"]) for key in dsMap.keys()]
	objs = system.tag.readBlocking(paths)
	print paths
	datasets = { dsName: obj.value for dsName, obj in zip(dsNames, objs) }
	# Split machine_roll_detail into A & B
	rolls = datasets["machine_roll_detail"]
	a_detail = system.dataset.deleteRows(rolls,[i for i in range(rolls.rowCount) if rolls.getValueAt(i,"winderID") != "A"])
	b_detail = system.dataset.deleteRows(rolls,[i for i in range(rolls.rowCount) if rolls.getValueAt(i,"winderID") != "B"])
	datasets["machine_roll_detail_A"] = a_detail
	datasets["machine_roll_detail_B"] = b_detail
	return datasets


def create_scheduledEvents(lineNumber):
	dsMap = get_dsMap()
	roll_detail_config = dsMap["machine_roll_detail"]
	dsMap.pop("machine_roll_detail")
	dsMap["machine_roll_detail_A"] = roll_detail_config
	dsMap["machine_roll_detail_A"]["dsName"] = "machine_roll_detail_A"
	dsMap["machine_roll_detail_B"] = roll_detail_config
	dsMap["machine_roll_detail_B"]["dsName"] = "machine_roll_detail_B"
	datasets = get_datasets()
	print datasets
	foreground = system.gui.color(0,0,0,255)
	leadTime = 0
	leadcolor = system.gui.color(255,255,0,255)
	colNames = ["EventID","ItemID","StartDate","EndDate","Label","Foreground","Background","LeadTime","LeadColor","PctDone", "orderNumber", "eventIndex"]
	rows = []
	eventID = 0
	# for item,contents in dsMap.iteritems():
	for item, contents in dsMap.iteritems():
		itemName = item
		ds = datasets[contents["dsName"]]
		print item, contents["dsName"], ds
		for idx in range(ds.rowCount):
			background = system.gui.color(255,220,198,255)
			progressBackground = background
			progressFill = background
			startDate = ds.getValueAt(idx, contents["startDate"])
			endDate = ds.getValueAt(idx, contents["endDate"])
			if itemName == "erpRolls":
				if ds.getValueAt(idx,"itemScrapped"):
					background = system.gui.color("red")
				if any(ds.getValueAt(idx,colName) is None for colName in ("pitStartTime", "pitEndTime")):
					startDate = ds.getValueAt(idx, "prodDateStamp")
					endDate = system.date.addHours(startDate, 1)
					background = system.gui.color("blue")
			if any( dsName == itemName for dsName in ["machine_roll_detail_A", "machine_roll_detail_B"] ):
				if any(ds.getValueAt(idx,colName) == True for colName in ("milChanged","widthChanged","conversionError")):
					background = system.gui.color("red")
			row=[
				eventID
				,"L{}-{}".format(lineNumber, itemName)
				,startDate
				,endDate
				,str(ds.getValueAt(idx, contents["label"]))
				,foreground
				,background
				,leadTime
				,leadcolor
				,100.0
				,ds.getValueAt(idx,contents["orderNumber"])
				,idx if contents["id"] == "idx" else ds.getValueAt(idx, contents["id"])
			]
			rows.append(row)
			eventID += 1
	scheduledEvents = system.dataset.toDataSet(colNames, rows)
	return scheduledEvents


def create_downtimeEvents(lineNumber):
	dsMap = get_dsMap()
	ds = system.tag.readBlocking(["[client]oee/line/downtimeEvents"])[0].value
	foreground = system.gui.color(0,0,0,255)
	downtimeColor = system.gui.color(255,0,0,50)
	colNames = ["ItemID","StartDate","EndDate","Color","Layer", "orderNumber"]
	rows = []
	for idx in range(ds.rowCount):
		if ds.getValueAt(idx,"parentEventCode") > 0:
			row=[
				"L{}-{}".format(lineNumber, "orderStats")
				,ds.getValueAt(idx,"StartTime")
				,ds.getValueAt(idx,"EndTime")
				,downtimeColor
				,1
				,ds.getValueAt(idx,"WorkOrderUUID")
			]
			rows.append(row)
	downtimeEvents = system.dataset.toDataSet(colNames, rows)
	return downtimeEvents


def create_breakEvents(lineNumber):
	ds = system.tag.readBlocking(["[client]oee/line/downtimeEvents"])[0].value
	breakColor = system.gui.color(255,0,0)
	colNames = ["StartDate","EndDate","Color"]
	rows = []
	for idx in range(ds.rowCount):
		if ds.getValueAt(idx,"parentEventCode") > 0:
			row=[
				ds.getValueAt(idx,"StartTime")
				,ds.getValueAt(idx,"EndTime")
				,breakColor
			]
			rows.append(row)
	breakEvents = system.dataset.toDataSet(colNames, rows)
	return breakEvents


def get_dates(scheduledEvents, downtimeEvents):
	min_scheduled = max_scheduled = min_downtime = max_downtime = None
	if scheduledEvents.rowCount > 0:
		min_scheduled = min(filter(None, [row["StartDate"] for row in system.dataset.toPyDataSet(scheduledEvents)]))
		max_scheduled = max(filter(None, [row["EndDate"] for row in system.dataset.toPyDataSet(scheduledEvents)]))
	if downtimeEvents.rowCount > 0:
		min_downtime = min(filter(None, [row["StartDate"] for row in system.dataset.toPyDataSet(downtimeEvents)]))
		max_downtime = max(filter(None, [row["EndDate"] for row in system.dataset.toPyDataSet(downtimeEvents)]))
	minDate = min(filter(None, [min_scheduled, min_downtime]))
	maxDate = max(filter(None, [max_scheduled, max_downtime]))
	return minDate, maxDate


def getChartData(rc):
	startDate = system.tag.readBlocking(["[client]oee/plant/startDate"])[0].value
	endDate = system.tag.readBlocking(["[client]oee/plant/endDate"])[0].value
	lineNumber = rc.lineNumber
	items = create_items(lineNumber)
	scheduledEvents = create_scheduledEvents(lineNumber)
	downtimeEvents = create_downtimeEvents(lineNumber)
	breakEvents = create_breakEvents(lineNumber)
	rc.getComponent("Equipment Schedule").items = items
	rc.getComponent("Equipment Schedule").scheduledEvents = scheduledEvents
	rc.getComponent("Equipment Schedule").downtimeEvents = downtimeEvents
	rc.getComponent("Equipment Schedule").breakEvents = breakEvents
	minDate, maxDate = get_dates(scheduledEvents, downtimeEvents)
	rc.getComponent("Equipment Schedule").startDate = system.date.addHours(minDate, -8)
	rc.getComponent("Equipment Schedule").endDate = system.date.addHours(maxDate, 8)
	# event.source.parent.getComponent('Equipment Schedule').breakEvents
	return


def create_orderEvents_new(event):
	lineNumber = event.source.parent.getComponent('val_lineNumber').intValue
	orderNumber = event.source.parent.getComponent('val_orderNumber').intValue
	machineDB = "SuperiorSQL"
	glassDB = oee.util.get_glass_db()
	machine_orders = oee.roll_detail.get_machine_orders(glassDB, orderNumber)
	machine_indexes = oee.roll_detail.getIndexesByOrderNumber(machineDB, orderNumber)
	machine_roll_detail = oee.roll_detail.getIndexRollDetailByOrderNumber(glassDB, orderNumber)
	datasets = {
		"machine_orders": machine_orders
		,"machine_indexes": machine_indexes
		,"machine_roll_detail": machine_roll_detail
	}
	dsMap = {
		"machine_orders": {"dsName": "machine_orders", "id": "Orders_idx", "startDate": "orderStart", "endDate": "orderEnd", "label": "orderNumber"}
		,"machine_indexes": {"dsName": "machine_indexes", "id": "indextimes_ndx", "startDate": "startTime", "endDate": "endTime", "label": "Order_Number"}
		,"machine_roll_detail": {"dsName": "machine_roll_detail", "id": "rollDetail_idx", "startDate": "rollStart", "endDate": "rollEnd", "label": "orderNumber"}
	}
	# items
	colNames = ["ID","Label","StatusImagePath","Foreground","Background"]


	image = "Builtin/icons/24/media_play.png"
	bg_color = system.gui.color(0,0,0,255)
	fg_color = system.gui.color(192,192,192,255)

	rows = [["L4-machine_orders","L4-machine_orders",image,bg_color,fg_color],
			["L4-machine_indexes","L4-machine_indexes",image,bg_color,fg_color],
			["L4-machine_roll_detail","L4-machine_roll_detail",image,bg_color,fg_color]
			]

	items = system.dataset.toDataSet(colNames, rows)
	# scheduledEvents
	foreground = system.gui.color(0,0,0,255)
	background = system.gui.color(255,220,198,255)
	leadTime = 0
	leadcolor = system.gui.color(255,255,0,255)
	colNames = ["EventID","ItemID","StartDate","EndDate","Label","Foreground","Background","LeadTime","LeadColor","PctDone"]
	rows = []
	for item, contents in dsMap.iteritems():
		ds = datasets[contents["dsName"]]
		for idx in range(ds.rowCount):
			row = [
				idx if contents["id"] == "idx" else ds.getValueAt(idx,contents["id"])
				,"L{}-{}".format(lineNumber,item)
				,ds.getValueAt(idx,contents["startDate"])
				,ds.getValueAt(idx,contents["endDate"])
				,str(ds.getValueAt(idx,contents["label"]))
				,foreground
				,background
				,leadTime
				,leadcolor
				,100.0
			]
			rows.append(row)
	scheduledEvents = system.dataset.toDataSet(colNames,rows)

	event.source.parent.getComponent("Equipment Schedule 1").items = items
	event.source.parent.getComponent("Equipment Schedule 1").scheduledEvents = scheduledEvents

	minDate, maxDate = get_dates(scheduledEvents, scheduledEvents)
	event.source.parent.getComponent("Equipment Schedule 1").startDate = system.date.addHours(minDate, -8)
	event.source.parent.getComponent("Equipment Schedule 1").endDate = system.date.addHours(maxDate, 8)


	print minDate
	print maxDate

	return


def schedule_onEventClicked(self, itemId, eventId, event):
	if event.clickCount == 2:
		scheduledEvents = self.parent.getComponent("Equipment Schedule").scheduledEvents
		# print 'schedule_onEventClicked', scheduledEvents
		# print 'itemId', itemId
		# print 'eventId',eventId, type(eventId)
		orderNumber = scheduledEvents.getValueAt(int(eventId), "orderNumber")
		print orderNumber
		filterByOrder(self.parent, orderNumber)
	return


def filterByOrder(rc, orderNumber):
	scheduledEvents = rc.getComponent("Equipment Schedule").scheduledEvents
	downtimeEvents = rc.getComponent("Equipment Schedule").downtimeEvents


	print "filterByOrder: filtered_scheduledEvents"
	filtered_scheduledEvents = oee.util.filterDataset(scheduledEvents, ["orderNumber"], [orderNumber])
	print "filterByOrder: filtered_downtimeEvents"
	filtered_downtimeEvents = oee.util.filterDataset(downtimeEvents,["orderNumber"],[orderNumber])
	rc.getComponent("Equipment Schedule 1").items = rc.getComponent("Equipment Schedule").items
	rc.getComponent("Equipment Schedule 1").scheduledEvents = filtered_scheduledEvents
	rc.getComponent("Equipment Schedule 1").downtimeEvents = filtered_downtimeEvents
	minDate, maxDate = get_dates(filtered_scheduledEvents, filtered_downtimeEvents)
	rc.getComponent("Equipment Schedule 1").startDate = system.date.addHours(minDate, -2)
	rc.getComponent("Equipment Schedule 1").endDate = system.date.addHours(maxDate, 2)

	print minDate
	print maxDate

	return













