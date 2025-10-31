def visionWindowOpened(event):
	rc = system.gui.getParentWindow(event).getRootContainer()
	header = rc.getComponent("Header")
	info = rc.getComponent("Info")
	header.getComponent("ApplyTimeRange").enabled = False
	info.getComponent("LineRepeater").visible = False
	info.getComponent("Loading").visible = True
	set_plants(rc)
	set_initial_time_range(event)
	btn_ApplyRange(event)
	header.getComponent("ApplyTimeRange").enabled = True
	info.getComponent("LineRepeater").visible = True
	info.getComponent("Loading").visible = False
	return


def btn_ApplyRange(event):
	rc = system.gui.getParentWindow(event).getRootContainer()
	header = rc.getComponent("Header")
	tagpaths = {
		"[client]oee/plant/startDate": header.getComponent("Date Range").startDate,
		"[client]oee/plant/endDate": header.getComponent("Date Range").endDate
	}
	r = system.tag.writeBlocking(tagpaths.keys(), tagpaths.values())
	# refresh_data(event)
	refresh_data_no_timing(event)
	return


def set_initial_time_range(event):
	# Set references
	rc = system.gui.getParentWindow(event).getRootContainer()
	header = rc.getComponent("Header")
	now = system.date.now()
	startDate = system.date.addWeeks(now, -1)
	endDate = now
	# Write to window
	header.getComponent("Date Range").outerRangeStartDate = system.date.addMonths(now, -1)
	header.getComponent("Date Range").outerRangeEndDate = now
	header.getComponent("Date Range").startDate = startDate
	header.getComponent("Date Range").endDate = endDate
	return


def set_plants(rc):
	systemName = system.tag.readBlocking("[System]Gateway/SystemName")[ 0 ].value
	if systemName == "Ignition-BLM-SQL02":
		additionalClause = ""
	else:
		additionalClause = "AND gateway = '{}'".format(systemName)
	qry = """
	SELECT DISTINCT
		[plantID]
		,[plantName]
		,[plantCode3]
		,[sourceID]
		,[providerLocal]
		,[providerRemote]
		,[gateway]
		,[defaultHistorian]
		,[gatewayHostname]
	FROM [Glass].[soc].[plantDef]
	WHERE sourceID IN (1,3) AND lineLinkID > 0
	{additionalClause} 
	ORDER BY sourceID, plantID
	""".format(additionalClause=additionalClause)
	pyds = system.db.runQuery(qry, "glass")
	ddPlants = system.dataset.toDataSet(pyds)
	dropDown = rc.getComponent("Header").getComponent('PlantDropdown')
	dropDown.data = ddPlants
	dropDown.selectedIndex = 0
	r = set_location(ddPlants,0)
	return ddPlants


def plantDropDown_propertyChange(event):
	if event.propertyName in 'selectedStringValue':
		dd = event.source
		if dd.selectedIndex > -1:
			r = set_location(dd.data, dd.selectedIndex)
		refresh_data(event)
	return


def set_location(dropdown, selectedIndex):
	tagpaths = {
		"[client]oee/plant/sourceID": dropdown.getValueAt(selectedIndex,"sourceID"),
		"[client]oee/plant/plantID": dropdown.getValueAt(selectedIndex,"plantID"),
		"[client]oee/plant/plantName": dropdown.getValueAt(selectedIndex,"plantName"),
		"[client]oee/line/lineLinkID": 0,
		"[client]oee/line/lineNumber": 0
	}
	r = system.tag.writeBlocking(tagpaths.keys(),tagpaths.values())
	return r


def read_location():
	tagpaths = {
		"sourceID": "[client]oee/plant/sourceID",
		"plantID": "[client]oee/plant/plantID",
		"plantName": "[client]oee/plant/plantName",
		"lineLinkID": "[client]oee/line/lineLinkID",
		"lineNumber": "[client]oee/line/lineNumber",
		"startDate": "[client]oee/plant/startDate",
		"endDate": "[client]oee/plant/endDate",
	}
	objs = system.tag.readBlocking( tagpaths.values() )
	location = {}
	location.clear()
	for tag_key, obj in zip(tagpaths.keys(), objs):
		location[tag_key] = obj.value
	return location


def refresh_data_no_timing(event):
	"""
	Version 2
	Args:
		event:
	Returns:
	"""
	loc = read_location()
	glassDB = oee.util.get_glass_db()
	erpDB = "CharterSQL_RC"
	ds_orders = oee.roll_detail.getOrdersByDateRange(glassDB,loc["sourceID"], loc["plantID"],loc["startDate"], loc["endDate"])
	orderNumbers = tuple(filter(None,set(row[ "orderNumber" ] for row in ds_orders)))
	orderTracking = oee.roll_detail.getOrderTracking(glassDB, loc["sourceID"], orderNumbers)
	orderStats = oee.roll_detail.getOrderStats(glassDB, loc["sourceID"], orderNumbers)
	productionData = oee.roll_detail.getErpProductionData(erpDB, orderNumbers)
	productionItems = oee.roll_detail.getErpProductionItems(erpDB, orderNumbers)
	# downtimeEvents = oee.db.getDowntimeEvents(glassDB, loc["sourceID"], loc["plantID"], loc["startDate"], loc["endDate"])
	downtimeEvents = oee.roll_detail.getDowntimeEvents(glassDB,loc["sourceID"], orderNumbers)
	machine_orders = oee.roll_detail.get_machine_orders(glassDB, orderNumbers)
	machine_roll_detail = oee.roll_detail.get_machine_roll_detail(glassDB, orderNumbers)
	bad_pit_records = oee.roll_detail.validate_pit_records(productionItems, machine_roll_detail)
	tagpaths = {
		"[client]oee/plant/orderStats": system.dataset.toDataSet(orderStats),
		"[client]oee/plant/orderTracking": system.dataset.toDataSet(orderTracking),
		"[client]oee/plant/erpData": system.dataset.toDataSet(productionData),
		"[client]oee/plant/erpRolls": system.dataset.toDataSet(productionItems),
		"[client]oee/plant/downtimeEvents": system.dataset.toDataSet(downtimeEvents),
		"[client]oee/plant/machine_orders": system.dataset.toDataSet(machine_orders),
		"[client]oee/plant/machine_roll_detail": system.dataset.toDataSet(machine_roll_detail),
		"[client]oee/plant/bad_pit_records": bad_pit_records
	}
	r = system.tag.writeBlocking(tagpaths.keys(), tagpaths.values())
	oee_plant = get_plant_oee_v2(orderStats, productionItems, downtimeEvents)
	template_data = get_template_data(oee_plant, loc["sourceID"], loc["plantID"], loc["startDate"], loc["endDate"])
	tagpaths = {
		"[client]oee/plant/oeeData": oee_plant,
		"[client]oee/plant/template_data": template_data
	}
	r = system.tag.writeBlocking(tagpaths.keys(), tagpaths.values())
	return


def refresh_data(event):
	"""
	Version 2
	Args:
		event:

	Returns:

	"""

	def time_it(functionName, last_timestamp):
		time_now = system.date.now()
		intMilliseconds = system.date.millisBetween(last_timestamp,time_now)
		print "%s: %d milliseconds" % (functionName,intMilliseconds)
		return time_now

	loc = read_location()
	glassDB = oee.util.get_glass_db()
	erpDB = "CharterSQL_RC"
	print("#########################################################################################")
	print( "refresh_data: start")
	print("#########################################################################################")
	print("ds_orders", system.date.now())
	time_begin = system.date.now()
	time_diff = time_it("Start", system.date.now())
	ds_orders = oee.roll_detail.getOrdersByDateRange(glassDB,loc["sourceID"], loc["plantID"],loc["startDate"], loc["endDate"])
	time_diff = time_it("ds_orders", time_diff)
	orderNumbers = tuple(filter(None,set(row[ "orderNumber" ] for row in ds_orders)))
	time_diff = time_it("orderNumbers",time_diff)
	orderTracking = oee.roll_detail.getOrderTracking(glassDB, loc["sourceID"], orderNumbers)
	time_diff = time_it("orderTracking",time_diff)
	orderStats = oee.roll_detail.getOrderStats(glassDB, loc["sourceID"], orderNumbers)
	time_diff = time_it("orderStats",time_diff)
	productionData = oee.roll_detail.getErpProductionData(erpDB, orderNumbers)
	time_diff = time_it("productionData",time_diff)
	productionItems = oee.roll_detail.getErpProductionItems(erpDB, orderNumbers)
	time_diff = time_it("productionItems",time_diff)
	# downtimeEvents = oee.db.getDowntimeEvents(glassDB, loc["sourceID"], loc["plantID"], loc["startDate"], loc["endDate"])
	downtimeEvents = oee.roll_detail.getDowntimeEvents(glassDB,loc["sourceID"], orderNumbers)
	time_diff = time_it("downtimeEvents",time_diff)
	machine_orders = oee.roll_detail.get_machine_orders(glassDB, orderNumbers)
	time_diff = time_it("machine_orders",time_diff)
	machine_roll_detail = oee.roll_detail.get_machine_roll_detail(glassDB, orderNumbers)
	time_diff = time_it("machine_roll_detail",time_diff)
	print("#########################################################################################")
	print( "refresh_data: results")
	print("#########################################################################################")
	print(glassDB,loc["sourceID"], loc["plantID"],loc["startDate"], loc["endDate"])
	print("ds_orders", len(ds_orders), " records")
	print("orderNumbers",len(orderNumbers), " records")
	print("orderTracking",len(orderTracking), " records")
	print("orderStats",len(orderStats), " records")
	print("productionData",len(productionData), " records")
	print("productionItems",len(productionItems), " records")
	print("downtimeEvents",len(downtimeEvents), " records")
	print("machine_orders",len(machine_orders), " records")
	print("machine_roll_detail",len(machine_roll_detail), " records")
	tagpaths = {
		"[client]oee/plant/orderStats": system.dataset.toDataSet(orderStats),
		"[client]oee/plant/orderTracking": system.dataset.toDataSet(orderTracking),
		"[client]oee/plant/erpData": system.dataset.toDataSet(productionData),
		"[client]oee/plant/erpRolls": system.dataset.toDataSet(productionItems),
		"[client]oee/plant/downtimeEvents": system.dataset.toDataSet(downtimeEvents),
		"[client]oee/plant/machine_orders": system.dataset.toDataSet(machine_orders),
		"[client]oee/plant/machine_roll_detail": system.dataset.toDataSet(machine_roll_detail)
	}
	r = system.tag.writeBlocking(tagpaths.keys(), tagpaths.values())
	print tagpaths.keys()
	print tagpaths.values()
	time_diff = time_it("save tags",time_diff)
	oee_plant = get_plant_oee_v2(orderStats, productionItems, downtimeEvents)
	time_diff = time_it("oee_plant",time_diff)
	template_data = get_template_data(oee_plant, loc["sourceID"], loc["plantID"], loc["startDate"], loc["endDate"])
	time_diff = time_it("template_data",time_diff)
	#downtime_states = get_downtime_states(downtimeEvents)
	print("oee_plant",oee_plant.rowCount," records")
	print("template_data",template_data.rowCount," records")
	tagpaths = {
		"[client]oee/plant/oeeData": oee_plant,
		"[client]oee/plant/template_data": template_data
	}
	r = system.tag.writeBlocking(tagpaths.keys(), tagpaths.values())
	time_diff = time_it("Total Milliseconds",time_begin)
	return


def get_plant_oee(orderStats, erpData, downtimeEvents):
	""" Process OEE numbers
	Rules:
		Only one row per order per line
			combine run segments from same production line into a single row

	"""
	def create_dataset(data):
		from com.inductiveautomation.ignition.common.util import DatasetBuilder
		from java.lang import Integer, String, Boolean, Float
		from java.util import Date
		row_def = []
		row_def.append( [ "sourceID", Integer ] )
		row_def.append( [ "plantID", Integer ] )
		row_def.append( [ "lineLinkID", Integer ] )
		row_def.append( [ "lineNumber", Integer ] )
		row_def.append( [ "orderNumber", Integer ] )
		row_def.append( [ "orderStart", Date ] )
		row_def.append( [ "orderEnd", Date ] )
		row_def.append( [ "socID", Integer ] )
		row_def.append( [ "targetRate", Float ] )
		row_def.append( [ "totalWeight", Float ] )
		row_def.append( [ "scrapWeight", Float ] )
		row_def.append( [ "production_time", Float ] )
		row_def.append( [ "running_time", Float ] )
		row_def.append( [ "generic_downtime", Float ] )
		row_def.append( [ "planned_downtime", Float ] )
		row_def.append( [ "unplanned_downtime", Float ] )
		row_def.append( [ "total_downtime", Float ] )
		row_def.append( [ "actual_runtime", Float ] )
		row_def.append( [ "duration_hours", Float ] )
		row_def.append( [ "availability", Float ] )
		row_def.append( [ "performance", Float ] )
		row_def.append( [ "quality", Float ] )
		row_def.append( [ "oee", Float ] )
		row_def.append(["event_count", Integer])
		infoBuilder = DatasetBuilder.newBuilder()
		infoBuilder.colNames(col[0] for col in row_def)
		infoBuilder.colTypes(col[1] for col in row_def)
		for row in data:
			infoBuilder.addRow(row[col[0]] for col in row_def)
		ds = infoBuilder.build()
		return ds

	def get_weights(erpData, location, order_info):
		d = { "totalWeight": 0.1,
			 "scrapWeight": 0.1
				}
		for i in range(erpData.rowCount):
			if get_location(erpData, i) == location and erpData.getValueAt(i, "orderNumber") == order_info["orderNumber"]:
				d["totalWeight"] = erpData.getValueAt(i, 'totalWeight')
				d["scrapWeight"] = erpData.getValueAt(i, 'scrapWeight')
				return d
		return d

	data = []
	for idx in range(orderStats.rowCount):
		order_location = get_location(orderStats, idx)
		order_info = {
			"orderNumber": orderStats.getValueAt(idx, 'orderNumber'),
			"orderStart": orderStats.getValueAt(idx, 'graphStart'),
			"orderEnd": orderStats.getValueAt(idx, 'graphEnd'),
			"targetRate": orderStats.getValueAt(idx, 'socTarget'),
			"socID": orderStats.getValueAt(idx,'socID')

		}
		erp_dict = get_weights(erpData, order_location, order_info)
		event_dict = getAvailability(downtimeEvents, order_location, order_info)
		sourceID = orderStats.getValueAt(idx, 'sourceID')
		plantID = orderStats.getValueAt(idx, 'plantID')
		lineNumber = orderStats.getValueAt(idx, 'lineNumber')
		orderNumber = orderStats.getValueAt(idx, 'orderNumber')
		pyds = system.dataset.toPyDataSet(orderStats)
		duration_hours = sum([ (system.date.secondsBetween(row[ "graphStart" ],	row[ "graphEnd" ]) / 3600.0)
							   for row in pyds
							   		if row[ "sourceID" ] == sourceID
							   			and row[ "plantID" ] == plantID
							   			and row[ "lineNumber" ] == lineNumber
							   			and row[ "orderNumber" ] == orderNumber ])
		# if lineNumber == 1:
		# 	print 'duration_hours', duration_hours
		duration_hours = duration_hours if duration_hours is not None else 1.0
		duration_hours = duration_hours if duration_hours != 0 else 1.0
		targetRate = order_info["targetRate"] if order_info["targetRate"] > 0 else 1
		performance = ( ( (erp_dict["totalWeight"] - erp_dict["scrapWeight"]) / duration_hours )  / targetRate ) * 100.0
		# if lineNumber == 1:
		# 	print lineNumber, orderNumber, erp_dict["totalWeight"], erp_dict["scrapWeight"], duration_hours, targetRate
		quality = ((erp_dict["totalWeight"] - erp_dict["scrapWeight"]) / erp_dict["totalWeight"]) * 100.0
		oee = performance * quality * event_dict["availability"] / 10000.0
		row_dict = merge_dicts(order_location, order_info, erp_dict, event_dict)
		row_dict.update({
			"performance": performance,
			"quality": quality,
			"oee": oee
			})
		data.append(row_dict)
	oee_plant = create_dataset(data)
	return oee_plant


def get_plant_oee_v2(orderStats, erpRolls, downtimeEvents):
	""" Process OEE numbers
	Rules:
		Separate row for each run segment
	"""
	def create_dataset(data):
		from com.inductiveautomation.ignition.common.util import DatasetBuilder
		from java.lang import Integer, String, Boolean, Float
		from java.util import Date
		row_def = []
		row_def.append( [ "orderStats_ndx", Integer ] )
		row_def.append( [ "sourceID", Integer ] )
		row_def.append( [ "plantID", Integer ] )
		row_def.append( [ "lineLinkID", Integer ] )
		row_def.append( [ "lineNumber", Integer ] )
		row_def.append( [ "orderNumber", Integer ] )
		row_def.append( [ "orderStart", Date ] )
		row_def.append( [ "orderEnd", Date ] )
		row_def.append( [ "socID", Integer ] )
		row_def.append( [ "targetRate", Float ] )
		row_def.append( [ "totalWeight", Float ] )
		row_def.append( [ "scrapWeight", Float ] )
		row_def.append( [ "scrapPct", Float] )
		row_def.append( [ "production_time", Float ] )
		row_def.append( [ "running_time", Float ] )
		row_def.append( [ "generic_downtime", Float ] )
		row_def.append( [ "planned_downtime", Float ] )
		row_def.append( [ "unplanned_downtime", Float ] )
		row_def.append( [ "total_downtime", Float ] )
		row_def.append( [ "actual_runtime", Float ] )
		row_def.append( [ "duration_hours", Float ] )
		row_def.append( [ "availability", Float ] )
		row_def.append( [ "performance_erp", Float ] )
		row_def.append( [ "performance", Float ] )
		row_def.append( [ "quality", Float ] )
		row_def.append( [ "oee", Float ] )
		row_def.append( [ "event_count", Integer ] )
		row_def.append([ "rows_total", Integer ])
		row_def.append([ "rows_assigned", Integer ])
		infoBuilder = DatasetBuilder.newBuilder()
		infoBuilder.colNames(col[0] for col in row_def)
		infoBuilder.colTypes(col[1] for col in row_def)
		for row in data:
			infoBuilder.addRow(row[col[0]] for col in row_def)
		ds = infoBuilder.build()
		return ds

	def get_weights(erpRolls, location, order_info):

		def withinWindow(rowIndex, dateStart, dateEnd):
			prodTime = erpRolls.getValueAt(rowIndex, "prodDateStamp")
			pitStart = erpRolls.getValueAt(rowIndex,"pitStartTime")
			pitEnd = erpRolls.getValueAt(rowIndex,"pitEndTime")

			# print prodTime, pitStart, pitEnd, type(prodTime), type(pitStart), type(pitEnd)

			if prodTime is not None:
				try:
					if system.date.isBetween(prodTime, dateStart, dateEnd):
						return True
				except:
					print "Error at line 413", rowIndex, dateStart, dateEnd, prodTime


					return False
			if pitStart is not None:
				if system.date.isBetween(pitStart, dateStart, dateEnd):
					return True
			if pitEnd is not None:
				if system.date.isBetween(pitEnd, dateStart, dateEnd):
					return True
			return False

		d = { "totalWeight": 0.1,
			 "scrapWeight": 0.1,
			  "scrapPct": 0.0,
			  "rows_total": 0,
			  "rows_assigned": 0
			  }
		startTime = system.date.addHours(order_info["orderStart"], -4)
		endTime = system.date.addHours(order_info["orderEnd"], 4)
		rows_total = rows_assigned = 0
		for i in range(erpRolls.rowCount):
			if get_location(erpRolls, i) == location and erpRolls.getValueAt(i, "orderNumber") == order_info["orderNumber"]:
				d["rows_total"] += 1
				if withinWindow(i, startTime, endTime):
					d["rows_assigned"] += 1
					weight = erpRolls.getValueAt(i, "weight") if erpRolls.getValueAt(i, "weight") is not None else 0
					d["totalWeight"] += weight
					if erpRolls.getValueAt(i, "itemScrapped"):
						d["scrapWeight"] += weight
		d["scrapPct"] = d["scrapWeight"] / d["totalWeight"]
		# print order_info["orderNumber"], rows_total, rows_assigned, d
		return d

	data = []
	filteredRolls = None
	for idx in range(orderStats.rowCount):
		# print idx, orderStats.rowCount
		order_location = get_location(orderStats, idx)
		order_info = {
			"orderStats_ndx": orderStats.getValueAt(idx, 'orderStats_ndx'),
			"orderNumber": orderStats.getValueAt(idx, 'orderNumber'),
			"orderStart": orderStats.getValueAt(idx, 'graphStart'),
			"orderEnd": orderStats.getValueAt(idx, 'graphEnd'),
			"targetRate": orderStats.getValueAt(idx, 'socTarget'),
			"socID": orderStats.getValueAt(idx,'socID')
		}
		erp_dict = get_weights(erpRolls, order_location, order_info)
		event_dict = getAvailability(downtimeEvents, order_location, order_info)
		sourceID = orderStats.getValueAt(idx, 'sourceID')
		plantID = orderStats.getValueAt(idx, 'plantID')
		lineNumber = orderStats.getValueAt(idx, 'lineNumber')
		orderNumber = orderStats.getValueAt(idx, 'orderNumber')
		pyds = system.dataset.toPyDataSet(orderStats)
		duration_hours = sum([ (system.date.secondsBetween(row[ "graphStart" ],	row[ "graphEnd" ]) / 3600.0)
							   for row in pyds
							   		if row[ "sourceID" ] == sourceID
							   			and row[ "plantID" ] == plantID
							   			and row[ "lineNumber" ] == lineNumber
							   			and row[ "orderNumber" ] == orderNumber ])
		duration_hours = duration_hours if duration_hours is not None else 1.0
		duration_hours = duration_hours if duration_hours != 0 else 1.0
		targetRate = order_info["targetRate"] if order_info["targetRate"] > 0 else 1
		performance_erp = ( ( (erp_dict["totalWeight"] ) / duration_hours )  / targetRate ) * 100.0
		performance = orderStats.getValueAt(idx, 'performance') if orderStats.getValueAt(idx, 'performance') is not None else 0.0
		quality = ((erp_dict["totalWeight"] - erp_dict["scrapWeight"]) / erp_dict["totalWeight"]) * 100.0
		try:
			oee = performance * quality * event_dict["availability"] / 10000.0
		except:
			print idx, performance, quality, event_dict["availability"]
			oee = 0.0
		row_dict = merge_dicts(order_location, order_info, erp_dict, event_dict)
		row_dict.update({
			"performance_erp": performance_erp,
			"performance": performance,
			"quality": quality,
			"oee": oee
			})
		data.append(row_dict)
	oee_plant = create_dataset(data)
	return oee_plant


def tz_offset(t_stamp, plantID):
	if plantID in (1, 3, 4, 6):
		return system.date.addHours(t_stamp, 1)
	return t_stamp


def getAvailability(events, location, order_info):
	event_dict = {
		"running_time": 0,
		"generic_downtime": 0,
		"planned_downtime": 0,
		"unplanned_downtime": 0,
		"production_time": 0.1,
		"total_downtime": 0,
		"actual_runtime": 0,
		"availability": 0.0,
		"duration_hours": 0.1,
		"event_count": 0
	}
	# return event_dict
	orderStart = tz_offset(order_info["orderStart"], location["plantID"])
	orderEnd = tz_offset(order_info["orderEnd"], location["plantID"])
	if orderStart is not None and orderEnd is not None:
		event_dict["production_time"] = system.date.secondsBetween(orderStart, orderEnd)
	event_dict["event_count"] = 0
	for i in range(events.rowCount):
		event_duration = 0
		# Location matches
		if get_location(events, i) == location:
			if str(events.getValueAt(i, 'WorkOrderUUID')) == str(order_info["orderNumber"]):
				event_dict["event_count"] += 1
			event_start = events.getValueAt(i, 'StartTime')
			event_end = events.getValueAt(i, 'EndTime')
			event_type = events.getValueAt(i, 'ParentEventCode')
			event_id = events.getValueAt(i, 'ID')
			# The following events must be closed
			if event_start is not None and event_end is not None:
				# If event started before order but ended during current order
				# system.date.isBetween(target, start, end)
				# system.date.isAfter(date_1, date_2)
				# system.date.isBefore(date_1, date_2)
				if system.date.isBefore(event_start, orderStart) and system.date.isBetween(event_end, orderStart, orderEnd):
					event_duration = system.date.secondsBetween(orderStart, event_end)
					# if order_info["orderNumber"] == 459357:
					# 	print( event_id, event_duration, event_start, orderStart, event_end, orderEnd, "event started before order but ended during current order" )
				# If event happened during current order
				elif event_start > orderStart and event_end < orderEnd:
					event_duration = system.date.secondsBetween(event_start, event_end)
					# if order_info["orderNumber"] == 459357:
					# 	print( event_id, event_duration, "event happened during current order")
				# If event started before order but ended after current order
				elif event_start < orderStart and event_end > orderEnd:
					event_duration = system.date.secondsBetween(orderStart, orderEnd)
					# if order_info["orderNumber"] == 459357:
					# 	print( event_id ,event_duration, "event started before order but ended after current order")
			if event_type == 0:
				event_dict["running_time"] += event_duration
			elif event_type == 1:
				event_dict["generic_downtime"] += event_duration
			elif event_type == 2:
				event_dict["planned_downtime"] += event_duration
			elif event_type == 3:
				event_dict["unplanned_downtime"] += event_duration
	event_dict["total_downtime"] = event_dict["generic_downtime"] + event_dict["planned_downtime"] + event_dict["unplanned_downtime"]
	event_dict["actual_runtime"] = event_dict["production_time"] - event_dict["total_downtime"]
	# event_dict["duration_hours"] = event_dict["production_time"] / 3600000.0  # (1000 / 60 / 60)
	event_dict["duration_hours"] = event_dict["production_time"] / 3600.0  # (60 / 60)
	if event_dict["production_time"] > 0:
		event_dict["availability"] = float(event_dict["actual_runtime"]) / float(event_dict["production_time"]) * 100
	return event_dict


def getMultipleRuns(event):
	rc = system.gui.getParentWindow(event).getRootContainer()
	ds = rc.orderStats
	column = "orderNumber"
	summary = { columnValue: 0 for columnValue in list(set([row[column] for row in system.dataset.toPyDataSet(ds) ])) }
	for idx in range(ds.rowCount):
		if ds.getValueAt(idx, column) in summary.keys():
			summary[ ds.getValueAt(idx, column) ] += 1
	headers = [ column, "RunCount" ]
	rows = [ [ k, v ] for k, v in summary.iteritems() if v > 1]
	rc.multipleRuns = system.dataset.sort( system.dataset.toDataSet(headers, rows), 0 )
	return


def get_location(ds, idx):
	location = {'sourceID': ds.getValueAt(idx, 'sourceID'),
			'plantID': ds.getValueAt(idx, 'plantID'),
			'lineLinkID': ds.getValueAt(idx, 'lineLinkID'),
			'lineNumber': ds.getValueAt(idx, 'lineNumber')
			 }
	return location


def merge_dicts(*dict_args):
	"""
	Given any number of dictionaries, shallow copy and merge into a new dict,
	precedence goes to key-value pairs in latter dictionaries.
	"""
	result = {}
	for dictionary in dict_args:
		result.update(dictionary)
	return result


def get_template_data(oee_plant, sourceID, plantID, startDate, endDate ):
	# print 'get_template_data'
	headers = [
		"sourceID",
		"plantID",
		"lineNumber",
		"performanceOEE",
		"availabilityOEE",
		"qualityOEE",
		"oee",
		"orderCount",
		"startDate",
		"endDate",
	]
	py_oee = system.dataset.toPyDataSet(oee_plant)
	line_numbers = oee.util.get_line_numbers(sourceID, plantID)
	print "get_template_data", line_numbers
	data = []
	for line_number in line_numbers:
		orders_ran = [row["orderNumber"] for row in py_oee if row["lineNumber"] == line_number]
		total_run_count = len(orders_ran) if orders_ran is not None else 0
		actual_runtimes = sum([row["actual_runtime"] for row in py_oee if row["lineNumber"] == line_number])
		production_times = sum([row["production_time"] for row in py_oee if row["lineNumber"] == line_number])
		total_weights = sum([row["totalWeight"] for row in py_oee if row["lineNumber"] == line_number])
		scrap_weights = sum([row["scrapWeight"] for row in py_oee if row["lineNumber"] == line_number])
		duration_hrs = sum([row["duration_hours"] for row in py_oee if row["lineNumber"] == line_number])
		target_rates = sum([row["targetRate"] for row in py_oee if row["lineNumber"] == line_number])
		if duration_hrs == 0.0 or target_rates == 0.0:
			performance_value = 0.0
		else:
			performance_value = ( total_weights / duration_hrs ) / ( target_rates / total_run_count) * 100.0
			performance_avg = sum([row["performance"] for row in py_oee if row["lineNumber"] == line_number]) / total_run_count
			print "get_template_data", line_number, performance_value, performance_avg
		if production_times == 0.0:
			availability_value = 0.0
		else:
			availability_value = actual_runtimes / production_times * 100.0
		if total_weights + scrap_weights == 0.0:
			quality_value = 0.0
		else:
			quality_value = ( ( total_weights - scrap_weights) / total_weights )  * 100.0
		oee_value = performance_value * quality_value * availability_value / 10000.0
		row_data = [ sourceID, plantID, line_number, performance_value, availability_value, quality_value, oee_value, total_run_count, startDate, endDate ]
		# print 'get_template_data', sourceID, plantID, line_number, performance_value, availability_value, quality_value, oee_value, total_run_count
		data.append( row_data )
	template_data = system.dataset.toDataSet(headers, data)
	return template_data


def convert_millis(millis):
	minutes, seconds = divmod(millis/1000, 60)
	hours, minutes = divmod(minutes, 60)
	return hours, minutes, seconds


def getChangeover(line_downtime):
	"""
	Changeover data and aggregation of given LocationName between StartTime and EndTime
	Returns dataset including level, total, and average of each level of changeover
	"""
	level = 1
	now = system.date.now()
	# Iterate through each event code for changeover levels 1 - 6
	data = []
	level_headers = ['Order', 'Start Time', 'End Time', 'Hrs', 'Mins', 'Is Downtime', 'sum_millis', 'avg_millis']
	level_data = {
				"level_1": [],
				"level_2": [],
				"level_3": [],
				"level_4": [],
				"level_5": [],
				"level_6": []
				}
	tagpaths = {
				"[client]oee/line/level_1_data": None,
				"[client]oee/line/level_2_data": None,
				"[client]oee/line/level_3_data": None,
				"[client]oee/line/level_4_data": None,
				"[client]oee/line/level_5_data": None,
				"[client]oee/line/level_6_data": None
				}
	for eventCode in range(205, 211):
		# Get changeover level data
		level_key = "level_{}".format(level)
		f_columns = ["EventCode"]
		f_values = [eventCode]
		dataset = oee.util.filterDataset(line_downtime, f_columns, f_values)
		if dataset.rowCount > 0:
			totalMillis = 0
			for i in range(dataset.rowCount):
				order_number = dataset.getValueAt(i, 'WorkOrderUUID')
				evt_start = dataset.getValueAt(i, 'StartTime')
				evt_end = dataset.getValueAt(i, 'EndTime')
				end = dataset.getValueAt(i, 'EndTime') if evt_end is not None else now
				is_down = dataset.getValueAt(i, 'IsChangeoverDowntime')
				millis = system.date.millisBetween(evt_start, end)
				totalMillis += millis
				hours, minutes, seconds = convert_millis(millis)
				sum_millis = totalMillis
				avg_millis = sum_millis / dataset.rowCount
				totalStr = "%02d:%02d:%02d" % convert_millis(totalMillis)
				avgStr = "%02d:%02d:%02d" % convert_millis(int(totalMillis / dataset.rowCount))
				level_data[level_key].append([order_number, evt_start, evt_end, hours, minutes, bool(is_down), totalStr[:-3], avgStr[:-3] ])
			tagpaths["[client]oee/line/level_{}_data".format(level)] = system.dataset.toDataSet(level_headers, level_data[level_key] )
			totalStr = "%02d:%02d:%02d" % convert_millis(totalMillis)
			avgStr = "%02d:%02d:%02d" % convert_millis(int(totalMillis / dataset.rowCount))
			title = "Level {}".format(level)
			data.append([title, totalStr, avgStr])
		level += 1
	# Write level datasets
	r = system.tag.writeBlocking(tagpaths.keys(), tagpaths.values())
	headers = ["Level", "Total Time", "Average Time"]
	r = system.tag.writeBlocking(["[client]oee/line/changeover_avgs"], [system.dataset.toDataSet(headers, data)])
	return

def idx_duration(ds, row_index):
	startTime = ds.getValueAt(row_index, 'StartTime')
	endTime = ds.getValueAt(row_index, 'EndTime')
	endTime = endTime if endTime is not None else system.date.now()
	duration = system.date.minutesBetween(startTime, endTime)
	return duration


def get_duration(row):
	start_time = row["StartTime"]
	end_time = row["EndTime"]
	end_time = end_time if end_time is not None else system.date.now()
	duration = system.date.minutesBetween(start_time, end_time)
	return duration


def get_downtime_occurrences(line_downtime):
	stateList = []
	occDict = {}
	durDict = {}
	# Find every instance of Unplanned Downtime
	dt_events = oee.util.filterDataset(line_downtime, ["IsDowntime"], [1])
	for idx in range(dt_events.getRowCount()):
		if dt_events.getValueAt(idx, 'IsDowntime') == 1:  # and data.getValueAt(row, 'IsPlanned') == 0:
			# Read values and calculate duration
			stateName = str(dt_events.getValueAt(idx, 'Name'))
			duration = idx_duration(dt_events, idx)
			# If this is a new state name, add it to the list and set initial dictionary values.
			if stateName not in stateList:
				stateList.append(stateName)
				occDict[stateName] = 1
				durDict[stateName] = duration
			# If this is a repeat state name, increment dictionary values by the appropriate amount.
			else:
				occDict[stateName] += 1
				durDict[stateName] += duration
	stateList.sort()
	occHeaders = ["State", "Occurrences"]
	occData = [ [ stateName, occDict[stateName] ] for stateName in stateList ]
	dt_occurrences = system.dataset.sort(system.dataset.toDataSet(occHeaders, occData), "Occurrences", 0)
	durData = [ [stateName, durDict[stateName] ] for stateName in stateList]
	sumData = [ [stateName, occDict[stateName], durDict[stateName] ] for stateName in stateList]
	# print(system.dataset.toDataSet(["State", "Occurrences"], occData))
	# print(durData)
	r = system.tag.writeBlocking(["[client]oee/line/dt_occurrences",
							  "[client]oee/line/dt_duration",
							  "[client]oee/line/dt_summary"
							  ], [dt_occurrences,
								  system.dataset.toDataSet(["State", "Duration"], durData),
								  system.dataset.toDataSet(["State", "Occurrences", "Duration Minutes"], sumData)
								  ])
	return


def template_clicked(event):
	""" Opens DetailedLinePopup
	sourceID, plantID, lineNumber comes from template clicked

	"""
	# Get template clicked data
	self = event.source.parent
	sourceID = self.sourceID
	plantID = self.plantID
	lineNumber = self.lineNumber
	# read, filter and write line data
	filterColumns = ["sourceID", "plantID", "lineNumber"]
	filterValues = [sourceID, plantID, lineNumber]
	dsNames = [
		"downtimeEvents",
		"erpData",
		"erpRolls",
		"oeeData",
		"orderStats",
		"orderTracking",
		"template_data",
		"machine_orders",
		"machine_roll_detail"
	]
	objs = system.tag.readBlocking( [ "[client]oee/plant/{}".format(dsName) for dsName in dsNames ] )
	ds_filtered = [ oee.util.filterDataset(obj.value, filterColumns, filterValues) for obj in objs ]
	system.tag.writeBlocking(["[client]oee/line/{}".format(dsName) for dsName in dsNames], ds_filtered)
	system.tag.writeBlocking(["[client]oee/line/lineNumber"], [lineNumber])
	oee.vision.equipmentSchedule.getScheduleData(lineNumber, "line")
	# processing data for next window here because it is easier. (And I am being lazy)
	getChangeover(ds_filtered[0])
	get_downtime_occurrences(ds_filtered[0])
	getUnplannedDowntimeTimePercentage(ds_filtered[0])
	get_downtime_percentage(ds_filtered[0])
	system.nav.openWindow("OEE/Popups/DetailedLinePopup")
	# Update raw data results
	tblName = system.tag.readBlocking(["[client]oee/tblName"])[0].value
	tblValue = system.tag.readBlocking([tblName])[0].value
	system.tag.writeBlocking(["[client]oee/tblResults"], [tblValue])
	return


def get_downtime_percentage(line_downtime):
	event_codes = {
		0: "Running",
		1: "Generic Downtime",
		2: "Planned Downtime",
		3: "Unplanned Downtime"
	}
	total_duration = float(sum([get_duration(row) for row in system.dataset.toPyDataSet(line_downtime)]))
	data = []
	for code in event_codes.keys():
		events = oee.util.filterDataset(line_downtime, ["ParentEventCode"], [code])
		code_duration = float(sum([get_duration(row) for row in system.dataset.toPyDataSet(events)]))
		percent = code_duration / total_duration if total_duration > 0 else 0
		data.append( [ event_codes[code], percent ] )
	ds_out = system.dataset.toDataSet(["Label", "Value"], data)
	r = system.tag.writeBlocking(["[client]oee/line/downtime_percentage_all"], ds_out)
	return


def getUnplannedDowntimeTimePercentage(line_downtime):
	dt_events = oee.util.filterDataset(line_downtime, ["ParentEventCode"], [3])
	total_duration = float(sum([get_duration(row) for row in system.dataset.toPyDataSet(line_downtime)]))
	summary = get_summary(dt_events, "Name", filterNone=True)
	data = []
	for k, v in summary.iteritems():
		data.append( [ k, float(v) / total_duration ] )
	ds_out = system.dataset.toDataSet(["Label", "Value"], data)
	r = system.tag.writeBlocking(["[client]oee/line/downtime_percentage_unplanned"], ds_out)
	return


def get_summary(ds, column, filterNone=True):
	""" Creates a sum of values in a dataset based on a given column
	ds:	dataset
	column: (string) column name to sum
	Returns a dictionary in the form {column: value}
		First column: column category
		Second column: sum of values
	"""
	def includeValue(value):
		if filterNone:
			if value in (None, ""):
				return False
		return True
	pyds = system.dataset.toPyDataSet(ds)
	# Create dictionary of distinct column values and set inital count to 0
	summary = {columnValue: 0.0 for columnValue in
			   list(set([row[column] for row in pyds if includeValue(row[column])]))}
	# Process dataset, updating counts
	for row in pyds:
		if row[column] in summary.keys():
			summary[row[column]] += get_duration(row)
	return summary


def get_average_changeover_times(event):
	"""
	dataset = system.db.runNamedQuery('GMS/OEE/GetEventsByLineCodeAndTimeRange', params)
	SELECT * FROM oee.DowntimeEvents
	WHERE LocationName = :LocationName AND EventCode = :EventCode
		AND StartTime > :StartTime AND (EndTime < :EndTime OR EndTime IS NULL)

	"""
	# load changeover data to table and labels
	# level 1 - 6 changeover exist through event codes 205-210
	rc = system.gui.getParentWindow(event).getRootContainer()
	startTime = rc.startTime
	endTime = rc.endTime
	downtime_events = system.tag.readBlocking(["[client]oee/line/downtimeEvents"])[0].value
	code = 205
	line = str(system.gui.getParentWindow(event).getComponentForPath('Root Container').line)
	plant = system.gui.getParentWindow(event).getComponentForPath('Root Container').plant
	locationName = plant + '/Line' + line
	for level in range(1, 7):
		# initialize and query data for changeover data, ex: code:205 = changeover lvl 1
		# average and total time for changeover lvl, then display it in table and labels
		params = {'LocationName': locationName, 'EventCode': code, 'StartTime': start, 'EndTime': end}
		dataset = system.db.runNamedQuery('GMS/OEE/GetEventsByLineCodeAndTimeRange', params)
		headers = ['Order', 'Start Time', 'End Time', 'Hrs', 'Mins', 'Is Downtime']
		tableDataset = []
		totalMillis = 0
		if dataset.rowCount > 0:
			# iterate through each table/changeovers average, total, and workorder data
			for row in range(dataset.rowCount):
				order = dataset.getValueAt(row, 'WorkOrderUUID')
				orderStart = dataset.getValueAt(row, 'StartTime')
				isDown = dataset.getValueAt(row, 'IsChangeoverDowntime')
				if dataset.getValueAt(row, 'EndTime') is not None:
					orderEnd = dataset.getValueAt(row, 'EndTime')
					timeDiff = system.date.millisBetween(orderStart, orderEnd)
					totalMillis = totalMillis + timeDiff
					hours = system.date.hoursBetween(orderStart, orderEnd)
					if hours > 0:
						mins = system.date.minutesBetween(orderStart, orderEnd) % 60
					else:
						mins = system.date.minutesBetween(orderStart, orderEnd)
					tableDataset.append([order, orderStart, orderEnd, hours, mins, bool(isDown)])
			finalDataset = system.dataset.toDataSet(headers, tableDataset)
			avgMillis = float(totalMillis) / float(dataset.rowCount)
			avgTime = system.date.format(system.date.fromMillis(long(avgMillis)), 'mm:ss')
			avgHrs = avgMillis / 3600000
			avgTime = str(int(avgHrs)) + ':' + avgTime
			totalTime = system.date.format(system.date.fromMillis(long(totalMillis)), 'mm:ss')
			totalHrs = float(totalMillis) / 3600000
			totalTime = str(int(totalHrs)) + ':' + totalTime
			# set changeover level properties to calculations
			system.gui.getParentWindow(event).getComponentForPath(
				'Root Container.L' + str(level) + 'Avg').text = avgTime
			system.gui.getParentWindow(event).getComponentForPath(
				'Root Container.L' + str(level) + 'Total').text = totalTime
			system.gui.getParentWindow(event).getComponentForPath(
				'Root Container.L' + str(level) + 'Table').data = finalDataset
		code = code + 1
	return

