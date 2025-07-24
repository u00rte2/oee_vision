def get_order_detail_OLD(event):
	rc = system.gui.getParentWindow(event).getRootContainer()
	dsNames = [
		"downtimeEvents"
		,"erpData"
		,"erpRolls"
		,"oeeData"
		,"orderStats"
		,"orderTracking"
		,"machine_orders"
		,"machine_roll_detail"
	]
	objs = system.tag.readBlocking([ "[client]oee/orderNumber/{}".format(dsName) for dsName in dsNames ])
	for name, obj in zip(dsNames, objs):
		if name == "erpData":
			ds = oee.util.flipDataset(obj.value)
		else:
			ds = obj.value
		container = rc.getComponent("cnt_{}".format(name))
		container.getComponent("tbl_data").data = ds
	return


def get_order_detail(event):
	erpData_columns = [
					"sourceID"
					,"plantID"
					,"lineNumber"
					,"orderNumber"
					,"orderNumber_sales"
					,"orderQuantity"
					,"orderBalance"
					,"jobStatus"
					,"targetRollWeight"
					,"targetRollLength"
					,"runRolls"
					,"rollsAcross"
					,"prodStartDate"
					,"prodCompleteDate"
					,"itemMasterID"
					,"itemCode"
					,"productCode"
					,"targetWidth"
					,"targetMil"
					,"erpTarget"
					,"defaultTarget"
					,"prodComplete"
					,"prodStartBalance"
					,"prodStartFootage"
					,"prodStartQty"
					,"prodTotalQty"
					,"FeetPerPound"
					]

	erpRolls_columns = [
						"sourceID"
						,"plantID"
						,"lineNumber"
						,"orderNumber"
						,"prodOrderNumber"
						,"itemNumber"
						,"position"
						,"weight"
						,"length"
						,"itemScrapped"
						,"prodLineID"
						,"prodDateStamp"
						,"pitID"
						,"pitStartTime"
						,"pitEndTime"
						,"FinishedWeight"
						,"currentWeight"
					]
	orderNumber = event.source.parent.getComponent('val_orderNumber').intValue
	dsMap = {
		"downtimeEvents": {"id": 0,"filterColumn": "WorkOrderUUID", "filterValue": str(orderNumber)},
		"erpData": {"id": 1,"filterColumn": "orderNumber", "filterValue": orderNumber},
		"erpRolls": {"id": 2,"filterColumn": "orderNumber", "filterValue": orderNumber},
		"oeeData": {"id": 3,"filterColumn": "orderNumber", "filterValue": orderNumber},
		"orderStats": {"id": 4,"filterColumn": "orderNumber", "filterValue": orderNumber},
		"orderTracking": {"id": 5,"filterColumn": "orderNumber", "filterValue": orderNumber},
		"machine_orders": {"id": 6,"filterColumn": "orderNumber", "filterValue": orderNumber},
		"machine_roll_detail": {"id": 7,"filterColumn": "orderNumber", "filterValue": orderNumber}
	}
	dsNames = dsMap.keys()
	objs = system.tag.readBlocking(["[client]oee/line/{}".format(dsName) for dsName in dsNames])
	for obj, dsName in zip(objs,dsNames):
		if dsName == "downtimeEvents":
			ds = oee.util.filterDataset(obj.value, [dsMap[dsName]["filterColumn"]], [dsMap[dsName]["filterValue"]])
		else:
			ds = oee.util.filterDataset(obj.value, [dsMap[dsName]["filterColumn"]], [dsMap[dsName]["filterValue"]])
		tableName = "Power Table {}".format(dsMap[dsName]["id"])
		if dsName == "erpData":
			ds = oee.util.flipDataset(system.dataset.filterColumns(ds, erpData_columns))


		event.source.parent.getComponent(tableName).data = ds

	return


def create_category_chart(event):
	rollDetail = system.tag.readBlocking(["[client]oee/orderNumber/machine_roll_detail"])[0].value
	pyds = system.dataset.toPyDataSet(rollDetail)
	headers = ["Label","Min","Q1","Median","Q3","Max"]
	calculations = ["min","q1","median","q3","max"]
	parameters = ["Out","MilAct","MilSet","TwoSigma","Speed","Width"]
	datasets = {name: [] for name in parameters}
	for i, r in enumerate(pyds):
		for dsName in datasets:
			row = [r["{}{}".format(calculation,dsName)] for calculation in calculations]
			row.insert(0,i)
			datasets[dsName].append(row)
	for dsName in datasets:
		ds = system.dataset.toDataSet(headers,datasets[dsName])
		event.source.parent.getComponent("roll_{}".format(dsName)).title = dsName
		event.source.parent.getComponent("roll_{}".format(dsName)).Data = ds
	create_category_chart_orderStats(event)
	return


def create_category_chart_orderStats(event):
	rc = event.source.parent
	ds_orderStats = rc.getComponent('cnt_orderStats').getComponent('tbl_data').data
	headers = ["Label","Min","Q1","Median","Q3","Max"]
	calculations = ["min","q1","median","q3","max"]
	parameters = ["Out","MilAct","TwoSigma"]
	newRows = []
	datasets = {parameter: [] for parameter in parameters}
	for idx in range(ds_orderStats.rowCount):
		for parameter in parameters:
			# row = [r["{}{}".format(calculation,dsName)] for calculation in calculations]

			row = [ds_orderStats.getValueAt( idx, "{}{}".format(calculation, parameter) ) for calculation in calculations]
			row.insert(0,idx)
			datasets[parameter].append(row)


			# row = ["{}_{}".format(parameter,idx)]
			# for calculation in calculations:
			# 	newRow.append(ds_orderStats.getValueAt(idx,"{}{}".format(calculation,parameter)))
			# newRows.append(row)
			# datasets[parameter].append(row)
	for dsName in datasets:
		ds = system.dataset.toDataSet(headers,datasets[dsName])
		event.source.parent.getComponent("order_{}".format(dsName)).title = dsName
		event.source.parent.getComponent("order_{}".format(dsName)).Data = ds
	return

def createSummary_OLD():
	machine_orders = system.tag.readBlocking(["[client]oee/orderNumber/machine_orders"])[0].value
	machine_roll_detail = system.tag.readBlocking(["[client]oee/orderNumber/machine_roll_detail"])[0].value
	oeeData = system.tag.readBlocking(["[client]oee/orderNumber/oeeData"])[0].value
	erpData = system.tag.readBlocking(["[client]oee/orderNumber/erpData"])[0].value
	erpRolls = system.tag.readBlocking(["[client]oee/orderNumber/erpRolls"])[0].value
	orderStats = system.tag.readBlocking(["[client]oee/orderNumber/orderStats"])[0].value

	avg_target = round(sum(filter(None,[row["targetRate"] for row in system.dataset.toPyDataSet(oeeData)])),2)

	headers = ['Source','Performance',"Quality",'Weight','Scrap','Hours',"Average Target"]
	rows = []

	weight = None
	scrap = None
	duration_hours = round(sum(filter(None,[row["duration_hours"] for row in system.dataset.toPyDataSet(erpData)])),2)
	performance = None
	quality = None
	rows.append(['ERP Order',performance,quality,weight,scrap,duration_hours,None])

	duration_hours = round(sum(filter(None,[row["duration_hours"] for row in system.dataset.toPyDataSet(machine_orders)])),2)
	rows.append(['Machine Order',None,None,None,None,duration_hours,None])

	weight_a = round(sum(filter(None,[row["lbs_produced"] / 2 for row in system.dataset.toPyDataSet(machine_roll_detail) if row["winderID"] == "A"])),2)
	scrap_a = round(sum(filter(None,[row["lbs_produced"] / 2 for row in system.dataset.toPyDataSet(machine_roll_detail) if
									 row["winderID"] == "A" and (row["milChanged"] or row["widthChanged"])])),2)
	duration_hours_a = round(sum(filter(None,[row["duration_hours"] for row in system.dataset.toPyDataSet(machine_roll_detail) if row["winderID"] == "A"])),2)
	performance_a = round(weight_a / duration_hours_a / avg_target,2)
	quality_a = round((weight_a - scrap_a) / weight_a,2)
	rows.append(['Winder A',performance_a,quality_a,weight_a,scrap_a,duration_hours_a,None])

	weight_b = round(sum(filter(None,[row["lbs_produced"] / 2 for row in system.dataset.toPyDataSet(machine_roll_detail) if row["winderID"] == "B"])),2)
	scrap_b = round(sum(filter(None,[row["lbs_produced"] / 2 for row in system.dataset.toPyDataSet(machine_roll_detail) if
									 row["winderID"] == "B" and (row["milChanged"] or row["widthChanged"])])),2)
	duration_hours_b = round(sum(filter(None,[row["duration_hours"] for row in system.dataset.toPyDataSet(machine_roll_detail) if row["winderID"] == "B"])),2)
	performance_b = round(weight_b / duration_hours_a / avg_target,2)
	quality_b = round((weight_b - scrap_b) / weight_b,2)
	rows.append(['Winder B',performance_b,quality_b,weight_b,scrap_b,duration_hours_b,None])

	weight = round(sum(filter(None,[row["weight"] for row in system.dataset.toPyDataSet(erpRolls)])),2)
	scrap = round(sum(filter(None,[row["weight"] for row in system.dataset.toPyDataSet(erpRolls) if row["itemScrapped"]])),2)
	duration_hours = round(sum(filter(None,[row["duration_hours"] for row in system.dataset.toPyDataSet(oeeData)])),2)
	performance = round(weight / duration_hours / avg_target,2)
	quality = round((weight - scrap) / weight,2)
	rows.append(['ERP Rolls',performance,quality,weight,scrap,duration_hours,None])

	weight = round(sum(filter(None,[row["lbs_produced"] for row in system.dataset.toPyDataSet(orderStats)])),2)
	scrap = None
	duration_hours = round(sum(filter(None,[row["duration_hours"] for row in system.dataset.toPyDataSet(orderStats)])),2)
	performance = round(weight / duration_hours / avg_target,2)
	quality = None
	rows.append(['orderStats',performance,quality,weight,scrap,duration_hours,avg_target])

	dsOut = system.dataset.toDataSet(headers,rows)
	oee.util.ptc(dsOut)

	return


def createSummary():

	def get_sum(ds, colName, filterColumn=None, filterValue=None, scrap=False):
		if filterColumn is None:
			total = round(sum(filter(None,[row[colName] for row in system.dataset.toPyDataSet(ds)])),2)
		else:
			if scrap:
				total = round(sum(filter(None,[row[colName] for row in system.dataset.toPyDataSet(ds) if row[filterColumn] == filterValue and ( row["milChanged"] or row["widthChanged"] ) ] )),2)
			else:
				total = round(sum(filter(None,[row[colName] for row in system.dataset.toPyDataSet(ds) if row[filterColumn] == filterValue])),2)
		return total

	# Datasets
	machine_orders = system.tag.readBlocking(["[client]oee/orderNumber/machine_orders"])[0].value
	machine_roll_detail = system.tag.readBlocking(["[client]oee/orderNumber/machine_roll_detail"])[0].value
	oeeData = system.tag.readBlocking(["[client]oee/orderNumber/oeeData"])[0].value
	erpData = system.tag.readBlocking(["[client]oee/orderNumber/erpData"])[0].value
	erpRolls = system.tag.readBlocking(["[client]oee/orderNumber/erpRolls"])[0].value
	orderStats = system.tag.readBlocking(["[client]oee/orderNumber/orderStats"])[0].value

	avg_target = get_sum(oeeData, "targetRate")
	headers = ['Source','Performance',"Quality",'Weight','Scrap','Hours',"Average Target"]
	rows = []
	rows.append(["ERP Order", # source
				  None, # performance
				  None, # quality
				  None, # weight
				  None, # scrap
				  get_sum(erpData, "duration_hours"), # duration_hours
				  None # target
				  ])
	rows.append(["Machine Order", # source
				  None, # performance
				  None, # quality
				  None, # weight
				  None, # scrap
				  get_sum(machine_orders, "duration_hours"), # duration_hours
				  None # target
				  ])
	weight_a = get_sum(machine_roll_detail, "lbs_produced",filterColumn="winderID", filterValue="A") / 2
	scrap_a = get_sum(machine_roll_detail,"lbs_produced",filterColumn="winderID",filterValue="A", scrap=True) / 2
	duration_hours_a = get_sum(machine_roll_detail, "duration_hours",filterColumn="winderID", filterValue="A")
	rows.append(["Winder A", # source
				  round(weight_a / duration_hours_a / avg_target,2), # performance
				  round((weight_a - scrap_a) / weight_a,2), # quality
				  weight_a, # weight
				  scrap_a, # scrap
				  duration_hours_a, # duration_hours
				  None # target
				  ])
	weight_b = get_sum(machine_roll_detail, "lbs_produced",filterColumn="winderID", filterValue="B") / 2
	scrap_b = get_sum(machine_roll_detail,"lbs_produced",filterColumn="winderID",filterValue="B", scrap=True) / 2
	duration_hours_b = get_sum(machine_roll_detail, "duration_hours",filterColumn="winderID", filterValue="B")
	rows.append(["Winder B", # source
				  round(weight_b / duration_hours_b / avg_target,2), # performance
				  round((weight_b - scrap_b) / weight_b,2), # quality
				  weight_b, # weight
				  scrap_b, # scrap
				  duration_hours_b, # duration_hours
				  None # target
				  ])
	weight = get_sum(erpRolls, "weight")
	scrap = get_sum(erpRolls,"weight")
	duration_hours = get_sum(oeeData, "duration_hours")
	rows.append(["ERP Rolls", # source
				  round(weight / duration_hours / avg_target,2), # performance
				  round((weight - scrap) / weight,2), # quality
				  weight, # weight
				  scrap, # scrap
				  duration_hours, # duration_hours
				  None # target
				  ])
	weight = get_sum(orderStats, "lbs_produced")
	duration_hours = get_sum(orderStats, "duration_hours")
	rows.append(["orderStats", # source
				  round(weight / duration_hours / avg_target,2), # performance
				  None, # quality
				  weight, # weight
				  None, # scrap
				  duration_hours, # duration_hours
				  avg_target # target
				  ])
	dsOut = system.dataset.toDataSet(headers,rows)
	oee.util.ptc(dsOut)
	return dsOut

def merge_run_segments(event):
	def get_row(pyds, rowID):
		for row in pyds:
			if row["orderStats_ndx"] == rowID:
				return row

	def convert_value(oeeRow, colName):
		columns_to_hours = [
			"production_time"
			,"running_time"
			,"generic_downtime"
			,"planned_downtime"
			,"unplanned_downtime"
			,"total_downtime"
			,"actual_runtime"
		]
		if colName in columns_to_hours and oeeRow[colName] is not None:
			return oeeRow[colName] / 3600.0
		return oeeRow[colName]


	rc = system.gui.getParentWindow(event).getRootContainer()
	runSegments = system.dataset.toPyDataSet(rc.getComponent("cnt_run_segments").getComponent("tbl_data").data)
	oeeData = system.dataset.toPyDataSet(rc.getComponent("cnt_oeeData").getComponent("tbl_data").data)
	columns_to_add = [
		"totalWeight"
		,"scrapWeight"
		,"scrapPct"
		,"production_time"
		,"running_time"
		,"generic_downtime"
		,"planned_downtime"
		,"unplanned_downtime"
		,"total_downtime"
		,"actual_runtime"
		,"availability"
		,"performance_erp"
		,"quality"
		,"oee"
		,"event_count"
		,"rows_total"
		,"rows_assigned"
		]
	newRows = []
	for row in runSegments:
		newRow = list(row)
		oeeRow = get_row(oeeData, row["orderStats_ndx"])
		newRow.extend([ convert_value(oeeRow, colName) for colName in columns_to_add ])
		newRows.append(newRow)
	headers = list(runSegments.columnNames)
	headers.extend(columns_to_add)
	dsOut = system.dataset.toDataSet(headers, newRows)
	return dsOut




