def getSocDB():
	if system.tag.readBlocking(['[System]Gateway/SystemName'])[0].value == 'Ignition-BLM-SQL02':
		socDB = 'glass_cnfsql04'
	else:
		socDB = 'glass'
	#socDB = 'glass' # Use BL-SQL01 for dev also.
	return socDB


def getHistory(paths, startDate, endDate):
	"""
	Parameters:
		tagpaths [ [tagpath, aggregationMode ] ]
		startDate date
		endDate date
	Returns: values dataset for tagpaths (values are converted to string)
	"""
	aggregationModes = [ "Average" ]
	includeBoundingValues = True
	validatesSCExec = True
	noInterpolation = False
	ignoreBadQuality = False
	returnFormat = "Wide"
	returnSize = 0
	intervalMinutes = 1
	dsValues = system.tag.queryTagHistory(paths=paths,
										  aggregationModes=aggregationModes,
										  startDate=startDate,
										  endDate=endDate,
										  intervalMinutes=intervalMinutes,
#										  aliases=aliases,
										  includeBoundingValues=includeBoundingValues,
										  validatesSCExec=validatesSCExec,
										  noInterpolation=noInterpolation,
										  ignoreBadQuality=ignoreBadQuality,
										  #returnSize = returnSize,
										  returnFormat=returnFormat)
	return dsValues


def getPaths(sourceID, plantID, lineNumber):
	qry = """
	SELECT p.* 
		,t.tagpath AS machineSpeedPath
		,t2.tagpath AS machineWidthPath
	FROM soc.plantDef p
		LEFT JOIN soc.tagMapping t
			ON p.sourceID = t.sourceID
				AND p.plantID = t.plantID
				AND p.lineNumber = t.lineNumber
				AND t.fk_paramID = 41
		LEFT JOIN soc.tagMapping t2
			ON p.sourceID = t2.sourceID
				AND p.plantID = t2.plantID
				AND p.lineNumber = t2.lineNumber
				AND t2.fk_paramID = 47
	WHERE p.sourceID = {sourceID}
		AND p.plantID = {plantID} 
	ORDER BY sourceID, plantID, lineNumber	
	""".format(sourceID=sourceID, plantID=plantID)
	ds = system.db.runQuery(qry, database=getSocDB())
	for idx in range(ds.rowCount):
		if ds.getValueAt(idx, 'sourceID') == sourceID and ds.getValueAt(idx, 'plantID') == plantID and ds.getValueAt(idx, 'lineNumber') == lineNumber:
			throughputPath = ds.getValueAt(idx, 'throughputPath')
			milSetPath = ds.getValueAt(idx, 'machineMilSetPath')
			milActPath = ds.getValueAt(idx, 'machineMilActPath')
			twoSigmaPath = ds.getValueAt(idx, 'twoSigmaPath')
			orderPath = ds.getValueAt(idx, 'orderPath')
			minimumRate = ds.getValueAt(idx, 'minimumRate')
			speedPath = ds.getValueAt(idx, 'machineSpeedPath')
			widthPath = ds.getValueAt(idx, 'machineWidthPath')
			systemName = system.tag.readBlocking(["[System]Gateway/SystemName"])[0].value
			if systemName == "Ignition-BLM-SQL02":
				localProviderColumn = "providerRemote"
			else:
				localProviderColumn = "providerLocal"
			provider = ds.getValueAt(idx, localProviderColumn)
			throughputPath =  "[%s]%s" % ( provider, throughputPath )
			machineMilSetPath = "[%s]%s" % ( provider, milSetPath )
			machineMilActPath = "[%s]%s" % ( provider, milActPath )
			machine2SigmaPath = "[%s]%s" % ( provider, twoSigmaPath )
			machineOrderPath = "[%s]%s" % ( provider, orderPath )
			machineSpeedPath = "[%s]%s" % ( provider, speedPath )
			machineWidthPath = "[%s]%s" % ( provider, widthPath )
			return (throughputPath, machineMilSetPath, machineMilActPath, machine2SigmaPath, machineOrderPath, machineSpeedPath, machineWidthPath, minimumRate)
	return ('','','', '', '', 0)


def historyMap():
	colMap = {
			"output": 1
			,"milSet": 2
			,"milAct": 3
			,"2Sigma": 4
			,"orderNumber": 5
			,"speed": 6
			,"width": 7
		}
	return colMap

def filterDataset(dsValues, lowLimit, hiLimit, minimumRate):
	rowsToDelete = []
	dictRejected = {
					"milRejects": 0
					,"outputRejects": 0
					,"twoSigmaRejects": 0
					}
	colMap = historyMap()
	for idx in range(dsValues.rowCount):
		mil_OK = twoSigma_OK = True
		output = dsValues.getValueAt(idx, colMap["output"])
		milSet = dsValues.getValueAt(idx, colMap["milSet"])
		milAct = dsValues.getValueAt(idx, colMap["milAct"])
		twoSigma = dsValues.getValueAt(idx, colMap["2Sigma"])
		if output != None:
			output_OK = output > minimumRate
		if milAct != None and milSet != None:
			mil_OK = lowLimit <= milAct <= hiLimit and lowLimit <= milSet <= hiLimit
		if output != None:
			output_OK = output > minimumRate
		if twoSigma != None:
			twoSigma_OK = twoSigma > 0
		if any(test == False for test in [mil_OK, twoSigma_OK, output_OK]):
			rowsToDelete.append(idx)
		if not mil_OK:
			dictRejected["milRejects"] += 1
		if not output_OK:
			dictRejected["outputRejects"] += 1
		if not twoSigma_OK:
			dictRejected["twoSigmaRejects"] += 1
	dsFiltered = system.dataset.deleteRows(dsValues, rowsToDelete)
	return dsFiltered, dictRejected


def getTagStats(dsIn, statValues):
	def getStats(values):
		if len(values) == 0:
			return [0,0,0,0,0,0]

		minOut = min(values)
		q1 = system.math.percentile(values, 25)
		median = system.math.median( values )
		q3 = system.math.percentile(values, 75)
		maxOut = max(values)
		stdDev = system.math.standardDeviation(values)
		return [minOut, q1, median, q3, maxOut, stdDev]

	def round_half_up(n, decimals=0):
		import math
		multiplier = 10**decimals
		return math.floor(n * multiplier + 0.5) / multiplier

	colMap = historyMap()
	pydata = system.dataset.toPyDataSet(dsIn)
	usedMinutes = dsIn.rowCount
	statValues.extend( [usedMinutes] )
	outputs = filter(None, [ val[colMap["output"]] for val in pydata ] )
	milSet = filter(None,[ val[colMap["milSet"]] for val in pydata ] )
	milAct = filter(None, [ val[colMap["milAct"]] for val in pydata ] )
	twoSigma = filter(None, [ val[colMap["2Sigma"]] for val in pydata ] )
	speed = filter(None, [ val[colMap["speed"]] for val in pydata ] )
	width = filter(None,[ val[ colMap["width"] ] for val in pydata ])
	statValues.extend(getStats(list(map(round_half_up, outputs))))
	statValues.extend(getStats(milAct))
	statValues.extend(getStats(twoSigma))
	statValues.extend(getStats(speed))
	statValues.extend(getStats(width))
	# print milSet, min(milSet), max(milSet)
	min_milSet = min(milSet)
	max_milSet = max(milSet)
	milChanged = min_milSet != max_milSet
	min_widthSet = min(width)
	max_widthSet = max(width)
	widthChanged = min_widthSet != max_widthSet
	statValues.extend( [ min_milSet, max_milSet, milChanged, min_widthSet, max_widthSet, widthChanged ] )
	return statValues


def getOEE(dsValues, orderStart, orderEnd, socTarget, rollsAcross):
	rollWeight = lbs_produced = roll_length = 0.0
	colMap = historyMap()
	if dsValues.rowCount == 1:
		startRate = dsValues.getValueAt( 0, colMap["output"] )
		endRate = startRate
	elif dsValues.rowCount > 1:
		startRate = dsValues.getValueAt( 0, colMap["output"] )
		endRate = dsValues.getValueAt( dsValues.rowCount-1, colMap["output"] )
		for idx in range(dsValues.rowCount):
			try:
				lbs_produced += dsValues.getValueAt( idx, 1 ) / 60.0
			except:
				pass
			try:
				roll_length += dsValues.getValueAt( idx, 6 )
			except:
				pass
	else:
		startRate = endRate = 0.0
	runHours = system.date.minutesBetween( orderStart, orderEnd ) / 60.0
	try:
		rollWeight = lbs_produced / (rollsAcross * 2)
		performance = (lbs_produced/runHours)/socTarget * 100
	except:
		performance = 0
	return [ lbs_produced, rollWeight, performance, startRate, endRate, roll_length ]


def erpCompliant(runningDict, lineKey):
	def noneToZero(value):
		""" Converts None type values to zero
		Parameters: value, any data
		"""
		if value is None:
			return 0.0
		return value

	def get_target_mil_used(runningDict,lineKey):
		value = runningDict[ lineKey ][ "mdoMil" ] or runningDict[ lineKey ][ "runMil" ] or runningDict[ lineKey ][ "targetMil" ]
		return noneToZero(value)

	def test_value(mil_erp,tolerance,value):
		try:
			deviation = float(mil_erp) - float(value)
			if abs(deviation) < tolerance:
				return True
			else:
				return False
		except:
			return True
		return False

	machine_mil_set = noneToZero(runningDict[ lineKey ][ "machineMilSet" ])
	machine_mil_act = noneToZero(runningDict[ lineKey ][ "machineMilAct" ])
	mil_erp = get_target_mil_used(runningDict,lineKey)
	percent_tolerance = 0.05
	tolerance = mil_erp * percent_tolerance + 0.005
	result = test_value(mil_erp,tolerance,machine_mil_set) and test_value(mil_erp,tolerance,machine_mil_act)
	return result


def saveData(statValues):
	databaseConnection = "glass"
	databaseTable = "soc.rollStats"
	columnNames = """(	"orderStatsID"
						,pitID
						,"sourceID"
						,"plantID"
						,"lineLinkID"
						,"lineNumber"
						,"orderNumber"
						,"sumMinutes"
						,"usedMinutes"
						,"minOut"
						,"q1Out"
						,"medianOut"
						,"q3Out"
						,"maxOut"
						,"stdDevOut"
						,"minMilAct"
						,"q1MilAct"
						,"medianMilAct"
						,"q3MilAct"
						,"maxMilAct"
						,"stdDevMilAct"
						,"minTwoSigma"
						,"q1TwoSigma"
						,"medianTwoSigma"
						,"q3TwoSigma"
						,"maxTwoSigma"
						,"stdDevTwoSigma"
						,"milRejects"
						,"outputRejects"
						,"twoSigmaRejects"
						,"lbs_produced"
						,"performance"
						,"startRate"
						,"endRate"
						,"setpointChange"
						)"""
	# placeholders = "(" + ",".join( ["?"] * len( statValues ) ) + ")"
	# args = []
	# totalLength = 0
	# for item in statValues:
	# 	args.append(item)
	# 	totalLength += len(str(item))
	# query = 'INSERT INTO %s %s VALUES %s' % (databaseTable, columnNames, placeholders)
	# system.db.runPrepUpdate(query, args, databaseConnection)
	return


def calculations(values):
	calcs = {
				"min": min(values)
				,"q1": system.math.percentile(values,25)
				,"median": system.math.median(values)
				,"q3": system.math.percentile(values,75)
				,"max": max(values)
				,"stdDev": system.math.standardDeviation(values)
			}
	return calcs


def data_definition():
	data = {
			"rollInfo": { "columnNames": [ "orderStatsID","lineLinkID","lineNumber","orderNumber_stats","orderNumber_pit","pitID","rollStart","rollEnd"]
						  }

			,"actuals": ["Out", "Mil", "TwoSigma", "Speed", "Width"]
			,"setpoints": [ "Mil", "Width" ]

			,"Out": {
					"min": 0.0
					,"q1": 0.0
					,"median": 0.0
					,"q3": 0.0
					,"max": 0.0
					,"stdDev": 0.0
					}
			,"Mil": {
					"minMilAct": 0.0
					,"q1MilAct": 0.0
					,"medianMilAct": 0.0
					,"q3MilAct": 0.0
					,"maxMilAct": 0.0
					,"stdDevMilAct": 0.0
					}
			,"TwoSigma": {
					"minTwoSigma": 0.0
					,"q1TwoSigma": 0.0
					,"medianTwoSigma": 0.0
					,"q3TwoSigma": 0.0
					,"maxTwoSigma": 0.0
					,"stdDevTwoSigma": 0.0
					}
			,"Speed": {
					"minSpeed": 0.0
					,"q1Speed": 0.0
					,"medianSpeed": 0.0
					,"q3Speed": 0.0
					,"maxSpeed": 0.0
					,"stdDevSpeed": 0.0
					}
			,"Width": {
				"minSpeed": 0.0
				,"q1Speed": 0.0
				,"medianSpeed": 0.0
				,"q3Speed": 0.0
				,"maxSpeed": 0.0
				,"stdDevSpeed": 0.0
			}

			,"setpoints": {
					"milSetMin": 0.0
					,"milSetMax": 0.0
					}


			,"usable_data": {
							"sumMinutes": 0
							,"usedMinutes": 0
							,"milRejects": 0.0
							,"outputRejects": 0.0
							,"twoSigmaRejects": 0.0
							}


		,"lbs_produced": 0.0
		,"rollWeight": 0.0
		,"rollLength": 0.0
		,"performance": 0.0
		,"startRate": 0.0
		,"endRate": 0.0
		,"milChanged": False


		,"order_probability": {
								"orderProbability": 0.0
								,"weightProbability": 0.0
								,"lengthProbability": 0.0
								,"milProbability": 0.0
					}


	}

	return

def get_confidence_order(orderNumber, rollStats):
	def getOrderIntFromString(rawOrder):
		""" Converts machine order number from a string to an integer
		It is possible to enter a list of order numbers at the machine.
		It is also possible to be a message such as 'Lip Clean'
		"""
		import itertools
		def getInteger(s):
			import unicodedata
			n = 0
			if s == None:
				return 0
			try:
				n = int(float(s))
				return n
			except ValueError:
				pass
			try:
				n = unicodedata.numeric(s)
				return n
			except (TypeError,ValueError):
				pass
			return n

		if len(rawOrder) < 3:
			rawOrder = '000'
		tempOrderList = [ "".join(x) for _,x in itertools.groupby(str(rawOrder),key=str.isdigit) ]
		order = [ ]
		orderInt = 0
		for item in tempOrderList:
			orderInt = getInteger(item)
			if orderInt > 0:
				order.append(orderInt)
		if len(order) > 0:
			# Process only the first order in the list. Subsequent processing will be a future feature
			orderInt = order[ 0 ]
		return orderInt

	rowID_start = None
	rowID_end = None
	for idx in range(rollStats.rowCount):
		orderNumber_pit = getOrderIntFromString(rollStats.getValueAt(idx,"orderNumber_pit"))
		if orderNumber_pit == orderNumber:
			if rowID_start is None:
				rowID_start = idx
				rowID_end = idx
		if rowID_start is not None and orderNumber_pit != orderNumber:
			rowID_end = idx
			# if idx > 1:
			# 	rowID_end = idx - 2
			# elif idx > 0:
			# 	rowID_end = idx - 1
			# else:
			# 	rowID_end = idx
			break
	transition_start = rowID_start - 2 if rollStats.rowCount > 2 else rowID_start
	transition_end = rowID_end + 2 if rowID_end + 1 < rollStats.rowCount else rowID_end
	print transition_start,rowID_start,rowID_end,transition_end
	order_confidence_levels = []
	for idx in range(rollStats.rowCount):
		if idx >= transition_start and idx <= rowID_start:
			c = 0.5
		elif idx > rowID_start and idx < rowID_end:
			c = 1.0
		elif idx >= rowID_end and idx <= transition_end:
			c = 0.5
		else:
			c = 0.0
		order_confidence_levels.append(c)
	return order_confidence_levels


def get_confidence_width(targetWidth, rollStats):
	c_width = []
	for idx in range(rollStats.rowCount):
		ratio = float(targetWidth) / rollStats.getValueAt(idx,"medianWidth")
		if ratio < 1.0:
			c_width.append(float(targetWidth) / rollStats.getValueAt(idx,"medianWidth"))
		else:
			c_width.append(0.0)
	return c_width


def get_confidence_mil(targetMil, rollStats):
	c_mil = []
	for idx in range(rollStats.rowCount):
		milSetMin = rollStats.getValueAt(idx, "milSetMin")
		milSetMax = rollStats.getValueAt(idx, "milSetMax")
		c_mil.append( (targetMil / milSetMin / 2.0) + (targetMil / milSetMax / 2.0))
	return c_mil


def processRolls(event):
	columnNames = [	"orderStatsID"
					,"lineLinkID"
					,"lineNumber"
					,"orderNumber_stats"
					,"orderNumber_pit"
					,"pitID"
					,"winderID"
					,"rollStart"
					,"rollEnd"
					,"sumMinutes"
					,"usedMinutes"
					,"minOut"
					,"q1Out"
					,"medianOut"
					,"q3Out"
					,"maxOut"
					,"stdDevOut"
					,"minMilAct"
					,"q1MilAct"
					,"medianMilAct"
					,"q3MilAct"
					,"maxMilAct"
					,"stdDevMilAct"
					,"minTwoSigma"
					,"q1TwoSigma"
					,"medianTwoSigma"
					,"q3TwoSigma"
					,"maxTwoSigma"
					,"stdDevTwoSigma"
					,"minSpeed"
					,"q1Speed"
					,"medianSpeed"
					,"q3Speed"
					,"maxSpeed"
					,"stdDevSpeed"
					,"minWidth"
					,"q1Width"
					,"medianWidth"
					,"q3Width"
					,"maxWidth"
					,"stdDevWidth"
					,"milSetMin"
					,"milSetMax"
					,"milChanged"
					,"widthSetMin"
					,"widthSetMax"
					,"widthChanged"
					,"milRejects"
					,"outputRejects"
					,"twoSigmaRejects"
					,"lbs_produced"
					,"rollWeight"
					,"performance"
					,"startRate"
					,"endRate"
					,"rollLength"
					]
	future = [ "erpCompliant"]
	rc = system.gui.getParentWindow(event).getRootContainer()
	tbl_orderStats = rc.getComponent("cnt_orderStats").getComponent("tbl_data")
	pyOrder = system.dataset.toPyDataSet(tbl_orderStats.data)
	orderRowIndex = tbl_orderStats.selectedRow
	baseData = [pyOrder.getValueAt(orderRowIndex, "orderStats_ndx"),
				pyOrder.getValueAt(orderRowIndex,"lineLinkID"),
				pyOrder.getValueAt(orderRowIndex,"lineNumber"),
				pyOrder.getValueAt(orderRowIndex,"orderNumber")
				]
	pyIndexes = system.dataset.toPyDataSet(rc.getComponent("cnt_indexesWindow").getComponent("tbl_data").data)
	rollsAcross = rc.getComponent("cnt_productionData").getComponent("tbl_data").data.getValueAt(0, "rollsAcross")
	i = 0
	rows = []
	for indexData in pyIndexes:
		newRow = list(baseData)
		newRow.extend( [ indexData["Order_Number"], indexData["indextimes_ndx"], indexData["winderNumber"] ] )

		print indexData["Order_Number"], indexData["indextimes_ndx"], indexData["winderNumber"]

		if i < 10000:
			stats, dsValues = processRoll(pyOrder, orderRowIndex, indexData, rollsAcross)
			newRow.extend( stats )
			rows.append( newRow )
			# print newRow
			i += 1
		#break
	rollStats = system.dataset.toDataSet( columnNames, rows )
	# Order Number Confidence
	order_confidence = get_confidence_order(pyOrder.getValueAt(orderRowIndex,"orderNumber"), rollStats)
	ds2 = system.dataset.addColumn(rollStats, order_confidence, "c_order", float)
	# Order Width Confidence
	c_width = get_confidence_width(pyOrder.getValueAt(orderRowIndex,"targetWidth"), rollStats)
	ds3 = system.dataset.addColumn(ds2, c_width, "c_width", float)
	# Order Mil Confidence
	targetMil = pyOrder.getValueAt(orderRowIndex,"targetMil")
	runMil = pyOrder.getValueAt(orderRowIndex,"runMil")
	mdoMil = pyOrder.getValueAt(orderRowIndex,"mdoMil")
	if mdoMil is not None:
		erpMil = mdoMil
	elif runMil is not None:
		erpMil = runMil
	else:
		erpMil = targetMil
	c_mil = get_confidence_mil(erpMil, rollStats)
	ds4 = system.dataset.addColumn(ds3, c_mil, "c_mil", float)

	rc.getComponent("cnt_rollStats").getBorder().setTitle("Roll Stats: {}".format(rollStats.rowCount))
	rc.getComponent("cnt_history").getBorder().setTitle("Roll Stats: {}".format(dsValues.rowCount))

	rc.getComponent("cnt_rollStats").getComponent("tbl_data").data = ds4
	rc.getComponent("cnt_history").getComponent("tbl_data").data = dsValues
	return


def processRoll(pyOrder, orderRowIndex, indexData, rollsAcross):
	"""
	"""
	logger = system.util.getLogger('oee Vision')
	maxDays = 10
	maxRunTime = maxDays * 24 * 60
	rollStart = indexData["startTime"]
	rollEnd = indexData["endTime"]
	if rollStart is None or rollEnd is None:
		logger.warn("rollStats: Invalid Dates: (%s), (%s)" % ( str(rollStart), str(rollEnd) ) )
		return
	minutesRunning = system.date.minutesBetween(rollStart, rollEnd)
	if minutesRunning < 1:
		logger.warn("rollStats: Negative Run Time: (%s), (%s)" % ( str(rollStart), str(rollEnd) ) )
		return
	elif minutesRunning > maxRunTime:
		logger.warn("rollStats: minutesRunning > maxRunTime: (%s), (%s)" % ( str(rollStart), str(rollEnd) ) )
		return
	elif pyOrder.getValueAt(orderRowIndex, "orderNumber") == 0:
		return
	elif pyOrder.getValueAt(orderRowIndex, "runMil") == None and pyOrder.getValueAt(orderRowIndex, "mdoMil") == None:
		logger.warn("rollStats: No targetMil.")
		return

	def runProcess(pyOrder, orderRowIndex, indexData, rollsAcross):
		# import system
		# from collections import namedtuple
		# Paths = namedtuple('Paths',["throughputPath", "machineMilSetPath", "machineMilActPath", "machine2SigmaPath", "machineOrderPath", "minimumRate"])
		rollStart = indexData[ "startTime" ]
		rollEnd = indexData[ "endTime" ]
		runMil = pyOrder.getValueAt(orderRowIndex, "runMil")
		mdoMil = pyOrder.getValueAt(orderRowIndex, "mdoMil")
		sourceID = pyOrder.getValueAt(orderRowIndex, "sourceID")
		plantID = pyOrder.getValueAt(orderRowIndex, "plantID")
		lineLinkID = pyOrder.getValueAt(orderRowIndex, "lineLinkID")
		lineNumber = pyOrder.getValueAt(orderRowIndex, "lineNumber")
		itemCode = pyOrder.getValueAt(orderRowIndex, "itemCode")
		productCode = pyOrder.getValueAt(orderRowIndex, "productCode")
		targetWidth = pyOrder.getValueAt(orderRowIndex, "targetWidth")
		targetMil = pyOrder.getValueAt(orderRowIndex, "targetMil")
		socTarget = pyOrder.getValueAt(orderRowIndex, "socTarget")
		orderError = pyOrder.getValueAt(orderRowIndex, "orderError")
		orderQuantity = pyOrder.getValueAt(orderRowIndex, "orderQuantity")
		machineTargetMil = max(filter(None, [ runMil, mdoMil ]))
		milTolerance = machineTargetMil * .05
		hiLimit = machineTargetMil + milTolerance
		lowLimit = machineTargetMil - milTolerance
		scope = "Gateway"
		throughputPath, machineMilSetPath, machineMilActPath, machine2SigmaPath, machineOrderPath, machineSpeedPath, machineWidthPath, minimumRate = getPaths(sourceID, plantID, lineNumber)
		paths = ( throughputPath, machineMilSetPath, machineMilActPath, machine2SigmaPath, machineOrderPath, machineSpeedPath, machineWidthPath )
		# print 'paths', paths, rollStart, rollEnd
		dsValues = getHistory(paths, rollStart, rollEnd)
		oeeValues = getOEE(dsValues, rollStart, rollEnd, socTarget, rollsAcross)
		dsFiltered, dictRejected = filterDataset(dsValues, lowLimit, hiLimit, minimumRate)
		# print type(dsValues), dsValues, dsFiltered, dictRejected
		# oee.util.ptc(dsValues)
		sumMinutes = system.date.minutesBetween( rollStart, rollEnd )
		rowData = [ rollStart
					,rollEnd
					,sumMinutes
					]
		statValues = getTagStats(dsValues, rowData)
		statValues.extend( [ dictRejected["milRejects"], dictRejected["outputRejects"], dictRejected["twoSigmaRejects"] ] )
		statValues.extend( oeeValues )
		# saveData(statValues)
		return statValues, dsValues

	statValues, dsValues = runProcess(pyOrder,orderRowIndex,indexData, rollsAcross)
	# system.util.invokeAsynchronous(runProcess, [pyOrder, orderRowIndex, orderStart, orderEnd, balanceStart, balanceEnd, orderRowID], "processOrderChange" )
	return statValues, dsValues


def rollStats_configureCell(self, value, textValue, selected, rowIndex, colIndex, colName, rowView, colView):
	"""
	Provides a chance to configure the contents of each cell. Return a
	dictionary of name-value pairs with the desired attributes. Available
	attributes include: 'background', 'border', 'font', 'foreground',
	'horizontalAlignment', 'iconPath', 'text', 'toolTipText',
	'verticalAlignment'

	You may also specify the attribute 'renderer', which is expected to be a
	javax.swing.JComponent which will be used to render the cell.

	Arguments:
		self: A reference to the component that is invoking this function.
		value: The value in the dataset at this cell
		textValue: The text the table expects to display at this cell (may be
		           overridden by including 'text' attribute in returned dictionary)
		selected: A boolean indicating whether this cell is currently selected
		rowIndex: The index of the row in the underlying dataset
		colIndex: The index of the column in the underlying dataset
		colName: The name of the column in the underlying dataset
		rowView: The index of the row, as it appears in the table view
		         (affected by sorting)
		colView: The index of the column, as it appears in the table view
		         (affected by column re-arranging and hiding)
	"""
	prodItems = self.parent.parent.getComponent('cnt_productionItems').getComponent('tbl_data').data
	if colName in ["c_order", "c_width", "c_mil"]:
		return { 'background': '0,255,0,{}'.format(value*255) }
	if self.data.getValueAt(rowIndex, "pitID") in [ prodItems.getValueAt(i, "pitID" ) for i in range(prodItems.rowCount) ]:
		return {'background': '#8AFF8A'}
	return