def configureCell(self, value, textValue, selected, rowIndex, colIndex, colName, rowView, colView):
# This example adds alternating background color:
#	if selected:
#		return {'background': self.selectionBackground}
#	elif rowView % 2 == 0:
#		return {'background': 'white'}
#	else:
#		return {'background': '#DDDDDD'}
	if colName == "ip":
		activeSessions = self.parent.active
		if oee.perspective.sessionExists(value, activeSessions):
			return {'background': 'yellow'}
		else:
			return {'background': 'white'}
	return