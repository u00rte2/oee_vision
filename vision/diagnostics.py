# Console test to check trigger status
# run from CNG-SCADA01
def ptc(dsIn):
	nrows = dsIn.getRowCount()
	ncols = dsIn.getColumnCount()
	rowLen = len(max(['row']+[unicode(i) for i in range(nrows)], key=len))
	colNames = dsIn.getColumnNames()
	headerList = []
	colData = []
	maxLen = []
	for i, col in zip(range(ncols), colNames):
		colIn = ([unicode(element) for element in list(dsIn.getColumnAsList(i))])
		maxLen = len(max([col]+colIn, key=len))
		colData.append([element.ljust(maxLen) for element in colIn])
		headerList.append(col.ljust(maxLen))
	headerString= 'row'.ljust(rowLen) + ' | ' + ' | '.join(headerList)
	print headerString
	print'-' * len(headerString)
	for row in enumerate(zip(*colData)):
		print unicode(row[0]).ljust(rowLen) + ' | ' + ' | '.join(row[1])
	return

def get_line_numbers():
	lineNumbers =  range(1,16) 
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
		kwargs = {"provider": provider, "plant":plantName, "line": lineNumber}
		path = "[{provider}]OT/{plant}/Production/Line{line}/OEE/Availability/NewDowntimeTrigger".format(**kwargs)
		tagpaths.append(path)
	objs = system.tag.readBlocking(tagpaths)
	for lineNumber, obj in zip(lineNumbers, objs):
		print "{}: Line {}, Trigger: {}".format( plantName, lineNumber, obj.value) 
	return
	
def getDowntimeDates():
	provider = get_provider()
	plantName = "Superior"
	tagpaths = []
	lineNumbers = get_line_numbers()
	for lineNumber in lineNumbers:
		kwargs = {"provider": provider, "plant":plantName, "line": lineNumber}
		path = "[{provider}]OT/{plant}/Production/Line{line}/OEE/Availability/DowntimeDate".format(**kwargs)
		tagpaths.append(path)
	objs = system.tag.readBlocking(tagpaths)
	now = system.date.now()
	for lineNumber, obj in zip(lineNumbers, objs):
		t_delta = system.date.minutesBetween(obj.value, now)
		print "{}: Line {}, Date: {} \t{}".format( plantName, lineNumber, obj.value, t_delta) 
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

if __name__ == "__main__":
	getDowntimeTriggers()
	getDowntimeDates()
	get_hosts()
	get_sessions()