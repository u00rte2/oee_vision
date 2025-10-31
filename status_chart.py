from org.jfree.chart.annotations import XYTextAnnotation
from java.awt import Color

def format_time(timestamp):
	days = int(timestamp / 86400)
	hours = int((timestamp % 86400) / 3600)
	minutes = int((timestamp % 3600) / 60)
	seconds = int(timestamp % 60)
	output = []
	if days > 0:
		output.append(str(days) + 'd')
	if hours > 0:
		output.append(str(hours) + 'h')
	if minutes > 0:
		output.append(str(minutes) + 'm')
	if seconds > 0:
		output.append(str(seconds) + 's')
	return ' : '.join(output) if output else '0s'


def generateLabel(data, plot, series, item, lastItemIndex):
	x = series - 1
	startDate = data.getValueAt(lastItemIndex, 0).getTime()
	endDate = data.getValueAt(item, 0).getTime()
	y = (float(startDate) + float(endDate)) * 0.5
	timeDiff = format_time((float(endDate) - float(startDate)) / 1000) #Time diff is being calculated here, but this could be literally anything
	annotation = XYTextAnnotation(timeDiff, x, y) #Timediff string added to chart
	annotation.setPaint(Color.BLACK)
	annotation.setFont(chartComponent.rangeAxisFont)
	plot.addAnnotation(annotation)
	return


def annotateTimestamp(event):
	chartComponent = event.source.parent.getComponent('Status Chart')
	data = chartComponent.data
	chart = chartComponent.chart
	plot = chart.XYPlot
	for annotation in plot.getAnnotations():
		if isinstance(annotation, XYTextAnnotation):
			plot.removeAnnotation(annotation)
	for series in range(1, data.columnCount):
		lastItemValue = data.getValueAt(0, series)
		nextItemValue = data.getValueAt(1, series)
		lastItemIndex = 0
		for item in range(1, data.rowCount):
			if lastItemValue != nextItemValue:
				generateLabel(data, plot, series, item, lastItemIndex)
				if (item + 1) < data.rowCount:
					lastItemValue = nextItemValue
					nextItemValue = data.getValueAt(item + 1, series)
					lastItemIndex = item
				else:
					generateLabel(data, plot, series, item, lastItemIndex)
			else:
				if (item + 1) < data.rowCount:
					nextItemValue = data.getValueAt(item + 1, series)
				else:
					generateLabel(data, plot, series, item, lastItemIndex)
	return


def getEventName(events, eventCode):
	for row in events:
		if row["EventCode"] == eventCode:
			return row["Name"]
			# parentName = row["ParentEventName"]
			# if parentName == "Running" or parentName == "Generic Downtime":
			# 	return row["Name"]
			# else:
			# 	return "{}: {}: {}".format( parentName, row["Category"], row["Name"] )
	return "{} Not Found".format( eventCode )

def getOrderNumber(defaultString, events):
	startingIndex = defaultString.index('(') + 1
	endingIndex = defaultString.index(')')
	datetimes = defaultString[ startingIndex: endingIndex ].split(',')
	startDate = system.date.parse(datetimes[ 0 ] + datetimes[ 1 ],'MM/dd/yy hh:mm a')
	for idx in range(events.rowCount):
		chart_date_string = system.date.format(startDate, "MM/dd/yy HH:mm a")
		event_date_string = system.date.format(events.getValueAt( idx, "StartTime"), "MM/dd/yy HH:mm a")
		if chart_date_string == event_date_string:
			return events.getValueAt( idx, "WorkOrderUUID" )
	return "Order Number Not Found"


def getChartToolTipText(self,seriesIndex,selectedTimeStamp,timeDiff,selectedStatus,data,properties,defaultString):
	"""
	Returns a formatted tool tip String.

	Arguments:
		self: A reference to the component that is invoking this function.
		seriesIndex: The series index corresponding to the column in the
					 series dataset.
		selectedTimeStamp: The time stamp corresponding to the x value of the
						   displayed tooltip. The time stamp is the number of seconds since the
						   epoch.
		timeDiff: The width of the current status interval measured in seconds
				  since the epoch.
		selectedStatus: The status value corresponding to the x value of the
						displayed tooltip.
		data: The series dataset as a PyDataset.
		properties: The series properties dataset as a PyDataset.
		defaultString: The default tooltip string.
	"""
	# return defaultString
	# Break the default string apart and extract the start date and the end date
	if selectedStatus < 1000:
		# Replace seriesName: "EventCode" with actual event name.
		events = system.dataset.toPyDataSet(self.parent.parent.parent.parent.parent.downtime)
		eventName = getEventName(events, selectedStatus)
		customString = defaultString.replace("EventCode", eventName)
		# Add order number
		customString = "{orderNumber}: {existingString}".format( orderNumber=getOrderNumber(defaultString, events), existingString=customString )
		return customString
	else:
		return "Order Number: {}, Duration: {} Hours".format(selectedStatus, oee.util.round_half_up(timeDiff/3600.0, decimals=1))