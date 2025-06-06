def get_order_detail(event):
	rc = system.gui.getParentWindow(event).getRootContainer()
	dsMap = {
				"downtimeEvents": {"columnName": "WorkOrderUUID", "columnValue": rc.orderNumber},
				"erpData": {"columnName": "orderNumber", "columnValue": rc.orderNumber},
				"oeeData": {"columnName": "orderNumber", "columnValue": rc.orderNumber},
				"orderStats": {"columnName": "orderNumber", "columnValue": rc.orderNumber},
				"template_data": {"columnName": "lineNumber", "columnValue": rc.lineNumber}
			}
	filterColumns = [ "orderNumber" ]
	dsNames = dsMap.keys()
	objs = system.tag.readBlocking([ "[client]oee/line/{}".format(dsName) for dsName in dsNames ])
	print objs
	#ds_filtered = [ (obj.value, dsMap[dsName]["columnName"], dsMap[dsName]["columnValue"]) for dsName, obj in zip(dsNames, objs) ]
	ds_filtered = [ oee.util.filterDataset(obj.value, [dsMap[ dsName ][ "columnName" ]] ,[dsMap[ dsName ][ "columnValue" ]]) for dsName,obj in zip(dsNames,objs) ]

	for name, ds in zip(dsNames, ds_filtered):
		print name, ds
	print
	print ds_filtered


	for name, ds in zip(dsNames, ds_filtered):
		container = rc.getComponent("cnt_{}".format(name))
		container.getComponent("tbl_data").data = oee.util.flipDataset(ds)



	return