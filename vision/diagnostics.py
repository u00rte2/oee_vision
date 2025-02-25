# Console test to check trigger status
# run from CNG-SCADA01
def ptc(dsIn):
	nrows = dsIn.getRowCount()
	ncols = dsIn.getColumnCount()
	rowLen = len(max(['row'] + [unicode(i) for i in range(nrows)], key=len))
	colNames = dsIn.getColumnNames()
	headerList = []
	colData = []
	maxLen = []
	for i, col in zip(range(ncols), colNames):
		colIn = ([unicode(element) for element in list(dsIn.getColumnAsList(i))])
		maxLen = len(max([col] + colIn, key=len))
		colData.append([element.ljust(maxLen) for element in colIn])
		headerList.append(col.ljust(maxLen))
	headerString = 'row'.ljust(rowLen) + ' | ' + ' | '.join(headerList)
	print( headerString )
	print( '-' * len(headerString) )
	for row in enumerate(zip(*colData)):
		print( unicode(row[0]).ljust(rowLen) + ' | ' + ' | '.join(row[1]) )
	return


def get_line_numbers():
	lineNumbers = range(1, 16)
	return lineNumbers


def get_provider():
	if system.tag.exists("[OEE]"):
		return "OEE"
	elif system.tag.exists("[Ignition_CNG_SCADA01_OEE]"):
		return "Ignition_CNG_SCADA01_OEE"
	return ""


def getDowntimeTriggers():
	provider = get_provider()
	plantName = "Superior"
	tagpaths = []
	lineNumbers = get_line_numbers()
	for lineNumber in lineNumbers:
		kwargs = {"provider": provider, "plant": plantName, "line": lineNumber}
		path = "[{provider}]OT/{plant}/Production/Line{line}/OEE/Availability/NewDowntimeTrigger".format(**kwargs)
		tagpaths.append(path)
	objs = system.tag.readBlocking(tagpaths)
	for lineNumber, obj in zip(lineNumbers, objs):
		print( "{}: Line {}, Trigger: {}".format(plantName, lineNumber, obj.value) )
	return


def getDowntimeDates():
	provider = get_provider()
	plantName = "Superior"
	tagpaths = []
	lineNumbers = get_line_numbers()
	for lineNumber in lineNumbers:
		kwargs = {"provider": provider, "plant": plantName, "line": lineNumber}
		path = "[{provider}]OT/{plant}/Production/Line{line}/OEE/Availability/DowntimeDate".format(**kwargs)
		tagpaths.append(path)
	objs = system.tag.readBlocking(tagpaths)
	now = system.date.now()
	for lineNumber, obj in zip(lineNumbers, objs):
		t_delta = system.date.minutesBetween(obj.value, now)
		print( "{}: Line {}, Date: {} \t{}".format(plantName, lineNumber, obj.value, t_delta) )
	return


def get_hosts():
	print("*****     hostConfig     *****")
	path = "[{provider}]OT/hostConfig".format(provider=get_provider())
	hostConfig = system.tag.readBlocking([path])[0].value
	ptc(hostConfig)
	return


def get_sessions():
	print("*****     sessions     *****")
	path = "[{provider}]OT/sessions".format(provider=get_provider())
	sessions = system.tag.readBlocking([path])[0].value
	ptc(sessions)
	return


def all_hosts_active(sessions):
	""" Verifies that all hosts are active.

	"""

	def send_email(ds):
		def get_table_html(ds):
			html = "<table cellpadding='2' cellspacing='2' border='1'><tr>"
			html += "<th>Plant</th>"
			html += "<th>Line</th>"
			html += "<th>oeeHostName</th>"
			html += "<th>ip</th>"
			html += "<th>faultTime</th>"
			for idx in range(ds.rowCount):
				html += "<tr>"
				html += "<td>" + str(ds.getValueAt(idx, "plantName")) + "</td>"
				html += "<td>" + str(ds.getValueAt(idx, "Line")) + "</td>"
				html += "<td>" + str(ds.getValueAt(idx, "oeeHostName")) + "</td>"
				html += "<td>" + str(ds.getValueAt(idx, "ip")) + "</td>"
				html += "<td>" + str(ds.getValueAt(idx, "faultTime")) + "</td>"
				html += "</tr>"
			html += "</table>"
			return html

		gateway = system.tag.readBlocking(['[System]Gateway/SystemName'])[0].value
		gateway = gateway.replace(" ", "_")
		version = system.util.getVersion()
		body = "<html><body>"
		body += "<h1>%s: Version %s Perspective Session Issue.</h1>" % (gateway, version)
		body += "<h3> Please review the application</h3>"
		body += "<h4>Sessions Not Found</h4>"
		body += get_table_html(ds)
		body += "</body></html>"
		recipients = [
			"Tim.Englund@cnginc.com",
			"Zach.Merrilees@cnginc.com",
		]
		system.net.sendEmail(smtp="mail.CharterNEX.com",
							 fromAddr="%s@cnginc.com" % gateway,
							 subject="Perspective Session Issue" % gateway,
							 body=body,
							 html=1,
							 to=recipients
							 )
		return

	def sessionExists(ipAddress, sessions):
		if sessions.columnCount is None:
			return True
		for idx in range(sessions.rowCount):
			if sessions.getValueAt(idx, "clientAddress") == ipAddress:
				return True
		return False

	def is_new_fault(oeeHostName):
		if ds_inactive_previous is None:
			return False
		for idx in range(ds_inactive_previous.rowCount):
			if oeeHostName == ds_inactive_previous.getValueAt(idx, "oeeHostName"):
				return False
		return True

	def any_fault_still_active(current_date):
		if ds_inactive_current is None:
			return False
		for idx in range(ds_inactive_current.rowCount):
			fault_date = ds_inactive_current.getValueAt(idx, "faultTime")
			fault_duration = system.date.minutesBetween(fault_date, current_date)
			if fault_duration % 60 == 0:
				return True
		return False

	ds_inactive_previous = system.tag.readBlocking(["[OEE]OT/inactiveHosts"])[0].value
	hostConfig = system.tag.readBlocking(["[OEE]OT/hostConfig"])[0].value
	inactive_rows = []
	current_date = system.date.now()
	notification = False
	headers = [
		"plantName"
		, "lineNumber"
		, "oeeLocation"
		, "oeeHostName"
		, "ip"
		, "faultTime"
	]
	for idx in range(hostConfig.rowCount):
		if not sessionExists(hostConfig.getValueAt(idx, "ip"), sessions):
			inactive_rows.append([
				hostConfig.getValueAt(idx, "plantName"),
				hostConfig.getValueAt(idx, "lineNumber"),
				hostConfig.getValueAt(idx, "oeeLocation"),
				hostConfig.getValueAt(idx, "oeeHostName"),
				hostConfig.getValueAt(idx, "ip"),
				current_date
			])
		if is_new_fault(hostConfig.getValueAt(idx, "oeeHostName")):
			notification = True
	if len(inactive_rows) > 0:
		ds_inactive_current = system.dataset.toDataSet(headers, inactive_rows)
	else:
		ds_inactive_current = None
	print("ds_inactive_current"), ds_inactive_current
	#	system.tag.writeBlocking(["[OEE]OT/inactiveHosts"], [ds_inactive_current])
	if any_fault_still_active(current_date):
		notification = True
	if notification:
		print("Missing Host:")
		for i in ds_inactive_current:
			print("Inactive ", i)
	# send_email(ds_inactive_current)
	return


def get_running_screens(schema):
	provider = get_provider()
	plantName = "Superior"
	tagpaths = []
	lineNumbers = get_line_numbers()
	for lineNumber in lineNumbers:
		kwargs = {"provider": provider, "plant": plantName, "line": lineNumber, "schema": schema}
		path = "[{provider}]{schema}/{plant}/Production/Line{line}/OEE/Availability/IsScreenRunning".format(**kwargs)
		tagpaths.append(path)
	objs = system.tag.readBlocking(tagpaths)
	for lineNumber, obj in zip(lineNumbers, objs):
		print(  "{}: {}: Line {}, IsScreenRunning: {}".format(schema, plantName, lineNumber, obj.value) )
	return


if __name__ == "__main__":
	getDowntimeTriggers()
	getDowntimeDates()
	get_hosts()
	get_sessions()
	path = "[{provider}]OT/sessions".format(provider=get_provider())
	sessions = system.tag.readBlocking([path])[0].value
	all_hosts_active(sessions)
	get_running_screens("CNG")
	get_running_screens("OT")






























