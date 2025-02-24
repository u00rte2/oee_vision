def visionWindowOpened(event):
	rc = system.gui.getParentWindow(event).getRootContainer()
	header = rc.getComponent("Header")
	info = rc.getComponent("Info")
	header.getComponent("ApplyTimeRange").enabled = False
	info.getComponent("LineRepeater").visible = False
	info.getComponent("Loading").visible = True
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
	refresh_data(event)
	return


def set_initial_time_range(event):
	# Set references
	rc = system.gui.getParentWindow(event).getRootContainer()
	header = rc.getComponent("Header")
	now = system.date.now()
	startDate = system.date.addWeeks(now, -2)
	endDate = now
	# Write to window
	header.getComponent("Date Range").outerRangeStartDate = system.date.addMonths(now, -1)
	header.getComponent("Date Range").outerRangeEndDate = now
	header.getComponent("Date Range").startDate = startDate
	header.getComponent("Date Range").endDate = endDate
	return



def getPlantDropDown(event):
	""" This is not used at this time.

		Returns:
			ddPlants: dataset of unique plants
	"""
	dsPlantConfig = system.tag.readBlocking(["[default]OT/SOC/config/plantConfiguration"])[0].value
	uniquePlants = list(set([ (row['sourceID'], row['plantID'] ) for row in system.dataset.toPyDataSet(dsPlantConfig) if row['lineLinkID'] != 0 ]))
	headers = [
				'plantName',
				'sourceID',
				'plantID',
				'erp',
				'providerLocal',
				'plantCode2',
				'plantCode3',
				'providerRemote',
				'gateway',
				'socDB',
				'defaultHistorian',
				'databaseProvider',
				'gatewayHostname'
				]
	rows = []
	for sourceID, plantID in uniquePlants:
		for row in system.dataset.toPyDataSet(dsPlantConfig):
			rowData = []
			if row['sourceID'] == sourceID and row['plantID'] == plantID:
				for col in headers:
					rowData.append(row[str(col)])
				if rowData not in rows:
					rows.append(rowData)
		dsPlants = system.dataset.toDataSet(headers, rows)
	dsPlants = system.dataset.sort(dsPlants, "sourceID")
	dsPlants = system.dataset.sort(dsPlants, "plantID")
	# Add an index for the drop down
	ddPlants = system.dataset.addColumn(dsPlants, 0, list(range(dsPlants.getRowCount())), 'index', int)
	if event is not None:
		rc = system.gui.getParentWindow(event).getRootContainer()
		header = rc.getComponent("Header")
		header.getComponent("PlantDropdown").data = getPlantDropDown(event)
	return ddPlants


def plantDropDown_propertyChange(event):
	if event.propertyName in 'selectedStringValue':
		dd = event.source
		if dd.selectedIndex > -1:
			tagpaths = {
				"[client]oee/plant/sourceID": dd.data.getValueAt(dd.selectedIndex, "sourceID"),
				"[client]oee/plant/plantID": dd.data.getValueAt(dd.selectedIndex, "plantID"),
				"[client]oee/plant/plantName": dd.data.getValueAt(dd.selectedIndex, "plantName"),
				"[client]oee/line/lineLinkID": 0,
				"[client]oee/line/lineNumber": 0
				}
			r = system.tag.writeBlocking( tagpaths.keys(), tagpaths.values() )
		refresh_data(event)
	return


def read_location():
	tagpaths = {
		"sourceID": "[client]oee/plant/sourceID",
		"plantID": "[client]oee/plant/plantID",
		"plantName": "[client]oee/plant/plantName",
		"lineLinkID": "[client]oee/line/lineLinkID",
		"lineNumber": "[client]oee/line/lineNumber",
		"startDate": "[client]oee/plant/startDate",
		"endDate": "[client]oee/plant/endDate",
		"schemaSource": "[client]oee/plant/schemaSource"
	}
	objs = system.tag.readBlocking( tagpaths.values() )
	location = {}
	location.clear()
	for tag_key, obj in zip(tagpaths.keys(), objs):
		location[tag_key] = obj.value
	return location


def refresh_data(event):
	loc = read_location()
	glass_db = oee.util.get_glass_db()
	orderStats = oee.db.getOrderStats(glass_db, loc["sourceID"], loc["plantID"], loc["startDate"], loc["endDate"])
	erpData = oee.db.getErpData("CharterSQL_RC", loc["sourceID"], loc["plantID"], loc["startDate"], loc["endDate"])
	if loc["schemaSource"] == "soc":
		downtimeEvents = oee.db.getDowntimeEvents(glass_db, loc["sourceID"], loc["plantID"], loc["startDate"], loc["endDate"])
	else:
		downtimeEvents = oee.db.get_gms_DowntimeEvents(glass_db, loc["sourceID"], loc["plantID"], loc["startDate"], loc["endDate"])
	oee_plant = get_plant_oee(orderStats, erpData, downtimeEvents)
	template_data = get_template_data(oee_plant, loc["sourceID"], loc["plantID"])
	#downtime_states = get_downtime_states(downtimeEvents)
	tagpaths = {
		"[client]oee/plant/orderStats": orderStats,
		"[client]oee/plant/erpData": erpData,
		"[client]oee/plant/downtimeEvents": downtimeEvents,
		"[client]oee/plant/oeeData": oee_plant,
		"[client]oee/plant/template_data": template_data
	}
	r = system.tag.writeBlocking(tagpaths.keys(), tagpaths.values())
	return


def get_plant_oee_OLD(orderStats, erpData, downtimeEvents):
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
			"targetRate": orderStats.getValueAt(idx, 'socTarget')
		}
		erp_dict = get_weights(erpData, order_location, order_info)
		event_dict = getAvailability(downtimeEvents, order_location, order_info)
		performance = ( ( (erp_dict["totalWeight"] - erp_dict["scrapWeight"]) / event_dict["duration_hours"] )  / order_info["targetRate"] ) * 100

		if performance > 1000:
			print 'Function: get_plant_oee '
			print 'order_info', order_info
			print
			print 'erp_dict', erp_dict
			print
			print 'performance', performance
			print
			print 'event_dict', event_dict

		quality = ((erp_dict["totalWeight"] - erp_dict["scrapWeight"]) / (erp_dict["totalWeight"] + erp_dict["scrapWeight"])) * 100
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
			"targetRate": orderStats.getValueAt(idx, 'socTarget')
		}
		erp_dict = get_weights(erpData, order_location, order_info)
		event_dict = getAvailability(downtimeEvents, order_location, order_info)

		sourceID = orderStats.getValueAt(idx, 'sourceID')
		plantID = orderStats.getValueAt(idx, 'plantID')
		lineNumber = orderStats.getValueAt(idx, 'lineNumber')
		orderNumber = orderStats.getValueAt(idx, 'orderNumber')

		pyds = system.dataset.toPyDataSet(orderStats)
		duration_hours = sum( [ (system.date.secondsBetween(row["graphStart"], row["graphEnd"]) / 3600.0 ) for row in pyds if row["sourceID"] == sourceID and row["plantID"] == plantID and row["lineNumber"] == lineNumber and row["orderNumber"] == orderNumber] )



		performance = ( ( (erp_dict["totalWeight"] - erp_dict["scrapWeight"]) / duration_hours )  / order_info["targetRate"] ) * 100
		quality = ((erp_dict["totalWeight"] - erp_dict["scrapWeight"]) / (erp_dict["totalWeight"] + erp_dict["scrapWeight"])) * 100
		oee = performance * quality * event_dict["availability"] / 10000.0
		row_dict = merge_dicts(order_location, order_info, erp_dict, event_dict)
		row_dict.update({
			"performance": performance,
			"quality": quality,
			"oee": oee
			})
		data.append(row_dict)

	# distinct_orders = list(set((row["sourceID"], row["plantID"], row["lineNumber"], row["orderNumber"] for row in system.dataset.toPyDataSet(orderStats))))
	# distinct_data = []
	# for sourceID, plantID, lineNumber, orderNumber in distinct_orders:
	# 	new_row_dict = {}
	# 	new_row_dict.clear()
	# 	for row_dict in data:
	# 		if row_dict["sourceID"] == sourceID and row_dict["plantID"] == plantID and row_dict["lineNumber"] == lineNumber and row_dict["orderNumber"] == orderNumber:
	# 			if len(new_row_dict) == 0:
	# 				new_row_dict.update(row_dict)
	# 			else:
	# 				new_row_dict.update(row_dict)
	#
	#
	#
	# 	pass



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
					if order_info["orderNumber"] == 459357:
						print( event_id, event_duration, event_start, orderStart, event_end, orderEnd, "event started before order but ended during current order" )
				# If event happened during current order
				elif event_start > orderStart and event_end < orderEnd:
					event_duration = system.date.secondsBetween(event_start, event_end)
					if order_info["orderNumber"] == 459357:
						print( event_id, event_duration, "event happened during current order")
				# If event started before order but ended after current order
				elif event_start < orderStart and event_end > orderEnd:
					event_duration = system.date.secondsBetween(orderStart, orderEnd)
					if order_info["orderNumber"] == 459357:
						print( event_id ,event_duration, "event started before order but ended after current order")
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


def get_template_data(oee_plant, sourceID, plantID):
	headers = [
		"sourceID",
		"plantID",
		"lineNumber",
		"performanceOEE",
		"availabilityOEE",
		"qualityOEE",
		"oee",
		"orderCount"
	]
	py_oee = system.dataset.toPyDataSet(oee_plant)
	line_numbers = oee.util.get_line_numbers(sourceID, plantID)
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
		if production_times == 0.0:
			availability_value = 0.0
		else:
			availability_value = actual_runtimes / production_times * 100.0
		if total_weights + scrap_weights == 0.0:
			quality_value = 0.0
		else:
			quality_value = ( ( total_weights - scrap_weights) / (total_weights + scrap_weights ) ) * 100.0
		oee_value = performance_value * quality_value * availability_value / 10000.0
		row_data = [ sourceID, plantID, line_number, performance_value, availability_value, quality_value, oee_value, total_run_count ]
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
	headers = ["Level", "Total", "Average"]
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
	occData = [ [ stateName, occDict[stateName] ] for stateName in stateList ]
	durData = [ [stateName, durDict[stateName] ] for stateName in stateList]
	sumData = [ [stateName, occDict[stateName], durDict[stateName] ] for stateName in stateList]

	print(system.dataset.toDataSet(["State", "Occurrences"], occData))
	print(durData)

	r = system.tag.writeBlocking(["[client]oee/line/dt_occurrences",
							  "[client]oee/line/dt_duration",
							  "[client]oee/line/dt_summary"
							  ], [system.dataset.toDataSet(["State", "Occurrences"], occData),
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
		"oeeData",
		"orderStats",
		"template_data",
	]
	objs = system.tag.readBlocking( [ "[client]oee/plant/{}".format(dsName) for dsName in dsNames ] )
	ds_filtered = [ oee.util.filterDataset(obj.value, filterColumns, filterValues) for obj in objs ]
	system.tag.writeBlocking(["[client]oee/line/{}".format(dsName) for dsName in dsNames], ds_filtered)
	system.tag.writeBlocking(["[client]oee/line/lineNumber"], [lineNumber])
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

