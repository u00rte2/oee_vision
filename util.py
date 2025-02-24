def ptc(datasetIn):
	""" Print Dataset To Console
			Jordan Clark
	"""
	nrows = datasetIn.getRowCount()
	ncols = datasetIn.getColumnCount()
	rowLen = len(max(['row']+[unicode(i) for i in range(nrows)], key=len))
	colNames = datasetIn.getColumnNames()
	headerList = []
	colData = []
	maxLen = []
	for i, col in zip(range(ncols), colNames):
		colIn = ([unicode(element) for element in list(datasetIn.getColumnAsList(i))])
		maxLen = len(max([col]+colIn, key=len))
		colData.append([element.ljust(maxLen) for element in colIn])
		headerList.append(col.ljust(maxLen))
	headerString= 'row'.ljust(rowLen) + ' | ' + ' | '.join(headerList)
	print(headerString)
	print('-' * len(headerString))
	for row in enumerate(zip(*colData)):
		print(unicode(row[0]).ljust(rowLen) + ' | ' + ' | '.join(row[1]))
	return


def getLocationData(plantID, lineNumber):
	""" Get sourceID, lineLinkID, oeeHostName, ipAddress
	"""
	hostConfig = system.tag.readBlocking([ "[~]OT/hostConfig" ])[0].value
	sourceID = lineLinkID = 0
	oeeHostname = oeeHostname = None
	for idx in range(hostConfig.rowCount):
		if hostConfig.getValueAt( idx, "plantID" ) == plantID and hostConfig.getValueAt( idx, "lineNumber" ) == lineNumber:
			sourceID = hostConfig.getValueAt( idx, "sourceID" )
			lineLinkID = hostConfig.getValueAt( idx, "lineLinkID" )
			oeeHostname = hostConfig.getValueAt( idx, "oeeHostname" )
			ipAddress = hostConfig.getValueAt( idx, "ip" )
	return sourceID, lineLinkID, oeeHostname, ipAddress


def isSessionActive(ipAddress):
	projectName = system.tag.readBlocking([ "[~]OT/projectName" ])[0].value
	activeSessions = system.tag.readBlocking([ "[~]OT/hostConfig" ])[0].value
	for idx in range(activeSessions.rowCount):
		if activeSessions.getValueAt( idx, "projectName" ) == projectName and hostConfig.getValueAt( idx, "ip" ) == ipAddress:
			return True
	return False


def get_line_numbers(sourceID, plantID):
	dsPlants = system.tag.readBlocking(["[default]OT/SOC/config/plantConfiguration"])[0].value
	line_numbers =  sorted(set([ row["lineNumber"]
							for row in system.dataset.toPyDataSet(dsPlants)
								if row["sourceID"] == sourceID and row["plantID"] == plantID
								]))
	return line_numbers


def filterDataset(dsIn, filterColumns, filterValues, sortKey=None, ascending=True):
	""" Filters Supplied Dataset

	parameters:
		dsIn, dataset to filter
		filterColumns, list of column names
		filterValues, list of column values
		sortKey, column name to sort [Optional]
		ascending, sort order [Optional]
			Note:
				If more than one column is specified, the operation is AND only. ie: sourceID and plantID
	returns: filtered dataset
	"""
	pyds = system.dataset.toPyDataSet(dsIn)
	rowsToDelete = list(set(( idx for idx, row  in enumerate(pyds) for col, val in zip(filterColumns, filterValues) if row[col] != val )))
	dsResults = system.dataset.deleteRows(dsIn, rowsToDelete)
	if sortKey != None:
		dsOut = system.dataset.sort(dsResults, sortKey, ascending)
	else:
		dsOut = dsResults
	return dsOut


def get_glass_db():
	localGatewayName = system.tag.readBlocking('[System]Gateway/SystemName')[0].value
	if localGatewayName == 'Ignition-BLM-SQL02':
		glass_db = "glass_cnfsql04"
	else:
		glass_db = "glass"
	return glass_db

def projectUpdate(actor, resources):
	""" This function is called from a gateway project update event.

		Parameter		Type		Desription
		actor			String		The user or system responsible for the update.
		resources		Dictionary	Contains the following keys
									added: List of dictionaries containing information about resources added to the project.
									removed: List of dictionaries containing information about resources removed from the project.
									modified: List of dictionaries containing information about resources that were modified.
									manifestChanged: A boolean indicating if a change was made to the project settings on the gateway.
		Returns:
			Nothing
	"""
	localGatewayName = system.tag.readBlocking('[System]Gateway/SystemName')[0].value
	projectName = system.project.getProjectName()
	if localGatewayName == 'Ignition-BLM-SQL02':
		databaseConnection = "glass_cnfsql04"
	else:
		databaseConnection = "glass"
	jsonLoad = system.util.jsonEncode(resources)
	t_stamp = system.date.format(system.date.now(),'MM-dd-yyyy hh:mm:ss a')
	qry = "INSERT INTO soc.project_updates ([gateway], [projectName], [actor], [resources], [t_stamp]) VALUES (?,?,?,?,?)"
	args = [localGatewayName, projectName, actor, jsonLoad, t_stamp]
	system.db.runPrepUpdate(qry, args, databaseConnection)
	return


