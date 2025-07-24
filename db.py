def getOrderStats(database, sourceID, plantID, startDate, endDate):
	qry = """
	SELECT 
		oStats.[orderStats_ndx]
		,oStats.[orderRowID]
		,oStats.[sourceID]
		,oStats.[plantID]
		,oStats.[lineLinkID]
		,oStats.[lineNumber]
		,oStats.[orderNumber]
		,oStats.[graphStart]
		,oStats.[graphEnd]
		,oStats.[socTarget]
		,oStats.[medianOut]
		,oStats.[itemCode]
		,oStats.[productCode]
		,oStats.[targetWidth]
		,oStats.[targetMil]
		,oStats.[socID]
		,oStats.[socRevLevel]
		,oStats.[orderQuantity]
		,oStats.[lbs_produced]
		,oStats.[balanceStart]
		,oStats.[balanceEnd]
		,oStats.[balanceStart] - oStats.[balanceEnd] AS lbsERP
		,oStats.[performance]
		,oStats.[startRate]
		,oStats.[endRate]
		,oStats.[t_stamp]
		,oStats.[sumMinutes]
		,oStats.[usedMinutes]
		,(DATEDIFF(SECOND,oStats.[graphStart],oStats.[graphEnd])) / 60.0 AS durationMinutes
		,oStats.[orderError]
		,CONCAT( oStats.[sourceID], oStats.[plantID], oStats.[lineNumber], oStats.[orderNumber]  ) AS uuid
		,1 AS runCount
	FROM [CNFSQL04].Glass.soc.orderStats oStats
	WHERE sourceID = ?
		AND plantID = ?
		AND graphStart BETWEEN ? AND ?
	ORDER BY sourceID, plantID, lineNumber, orderNumber
	"""
	dsOut = system.db.runPrepQuery(qry, [ sourceID, plantID, startDate, endDate ], database)
	return system.dataset.toDataSet(dsOut)


def getErpData(database, sourceID, plantID, startDate, endDate):
	qry = """
		SELECT TOP (50000)
				1 AS sourceID
				,COALESCE(SalesOrders.plantRoutingID, j.plantID) AS plantID
				,COALESCE(schedulemaster.linenumber, ProductionLineInfo.lineInfoID, j.lineLinkID) AS lineLinkID
				,COALESCE(ProductionLineInfo.linenumber, schline.linenumber, j.lineNumber) AS lineNumber
				,SalesOrders.ordernumber AS orderNumber_sales
				,j.orderNumber
				,SalesOrders.qty AS orderQuantity
				,ScheduleMASTER.orderBalance
				,COALESCE( (SELECT SUM(
										COALESCE( [weight], 0)
										) 
											FROM [CNFSQLProd01].[CharterSQL_RC].[dbo].productionitems 
											WHERE orderNumber = j.orderNumber 
												AND j.orderNumber != 0 
												AND prodLineID = j.lineLinkID
							), -1
							) AS totalWeight
				,COALESCE( (SELECT SUM(CASE 
										WHEN itemScrapped = 1 
										THEN COALESCE( [weight] , 0)
										ELSE 0 
									END) 
							FROM [CNFSQLProd01].[CharterSQL_RC].[dbo].productionitems 
							WHERE prodLineID = j.lineLinkID
								AND orderNumber = j.orderNumber 
								AND j.orderNumber != 0
							)
							,-1
						) AS scrapWeight
				,COALESCE( (SELECT COUNT([weight]) 
											FROM [CNFSQLProd01].[CharterSQL_RC].[dbo].productionitems 
											WHERE orderNumber = j.orderNumber 
												AND j.orderNumber != 0 
												AND prodLineID = j.lineLinkID
							), -1
							) AS rollCount
				,COALESCE( (SELECT COUNT([weight]) 
											FROM [CNFSQLProd01].[CharterSQL_RC].[dbo].productionitems 
											WHERE orderNumber = j.orderNumber 
												AND j.orderNumber != 0 
												AND prodLineID = j.lineLinkID
												AND itemScrapped = 1 
							), -1
							) AS scrapCount
				,j.runCount
				,SalesOrders.jobStatus
				,ProductionData.prodTargetRollWeight AS targetRollWeight
				,WorkOrder.runRolls
				,WorkOrder.rollsAcross
				,ProductionData.prodStartDate
				,ProductionData.prodCompleteDate
				,SalesOrders.itemMasterID
				,ItemMaster.itemMasterNumber AS itemCode
				,SalesOrders.cnumber AS productCode
				,SalesOrders.WidthInInches AS targetWidth
				,SalesOrders.mil AS targetMil
				,schedulemaster.lbsHour AS erpTarget
				,ProductionLineInfo.targetRate AS defaultTarget
				,ProductionData.prodComplete
		FROM [CNFSQLProd01].[CharterSQL_RC].[dbo].[salesorders] WITH (NOLOCK)
				JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].[ItemMaster] WITH (NOLOCK) ON SalesOrders.itemMasterID = ItemMaster.itemMasterID
				LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].productiondata WITH (NOLOCK) ON SalesOrders.ordernumber = productiondata.ordernumber 
				INNER JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].WorkOrder WITH (NOLOCK) ON SalesOrders.orderNumber = WorkOrder.orderNumber 
				LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].ProductionLineInfo WITH (NOLOCK) ON ProductionLineInfo.lineInfoID = productiondata.prodLineID
				LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].schedulemaster WITH (NOLOCK) ON schedulemaster.orderNumber = SalesOrders.orderNumber
				LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].ProductionLineInfo SchLine WITH (NOLOCK) ON schedulemaster.lineNumber = SchLine.lineInfoID
				RIGHT JOIN (
							SELECT DISTINCT
								[sourceID]
								,[plantID]
								,[lineLinkID]
								,[lineNumber]
								,[orderNumber]
								,COUNT([orderNumber]) as runCount
							FROM [CNFSQL04].Glass.soc.orderStats
							WHERE sourceID = ?
								AND plantID = ?
								--AND lineNumber = @lineNumber
								AND graphStart BETWEEN ? AND ?

							GROUP BY 	[sourceID]
								,[plantID]
								,[lineLinkID]
								,[lineNumber]
								,[orderNumber]
							) j
								ON j.sourceID = 1
									AND j.plantID = SalesOrders.plantRoutingID
									AND j.lineLinkID = COALESCE(schedulemaster.linenumber, ProductionLineInfo.lineInfoID, 0)
									AND j.orderNumber = SalesOrders.orderNumber
	ORDER BY lineNumber, orderNumber	
	"""
	dsOut = system.db.runPrepQuery(qry, [sourceID, plantID, startDate, endDate], database)
	return system.dataset.toDataSet(dsOut)


def getErpData_no_rolls(database, sourceID, plantID, startDate, endDate):
	qry = """
	SELECT TOP (50000)
			1 AS sourceID
			,COALESCE(SalesOrders.plantRoutingID, j.plantID) AS plantID
			,COALESCE(schedulemaster.linenumber, ProductionLineInfo.lineInfoID, j.lineLinkID) AS lineLinkID
			,COALESCE(ProductionLineInfo.linenumber, schline.linenumber, j.lineNumber) AS lineNumber
			,SalesOrders.ordernumber AS orderNumber_sales
			,j.orderNumber
			,SalesOrders.qty AS orderQuantity
			,ScheduleMASTER.orderBalance
			,SalesOrders.jobStatus
			,ProductionData.prodTargetRollWeight AS targetRollWeight
			,WorkOrder.runRolls
			,WorkOrder.rollsAcross
			,ProductionData.prodStartDate
			,ProductionData.prodCompleteDate
			,SalesOrders.itemMasterID
			,ItemMaster.itemMasterNumber AS itemCode
			,SalesOrders.cnumber AS productCode
			,SalesOrders.WidthInInches AS targetWidth
			,SalesOrders.mil AS targetMil
			,schedulemaster.lbsHour AS erpTarget
			,ProductionLineInfo.targetRate AS defaultTarget
			,ProductionData.prodComplete
	FROM [CNFSQLProd01].[CharterSQL_RC].[dbo].[salesorders] WITH (NOLOCK)
			JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].[ItemMaster] WITH (NOLOCK) ON SalesOrders.itemMasterID = ItemMaster.itemMasterID
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].productiondata WITH (NOLOCK) ON SalesOrders.ordernumber = productiondata.ordernumber 
			INNER JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].WorkOrder WITH (NOLOCK) ON SalesOrders.orderNumber = WorkOrder.orderNumber 
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].ProductionLineInfo WITH (NOLOCK) ON ProductionLineInfo.lineInfoID = productiondata.prodLineID
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].schedulemaster WITH (NOLOCK) ON schedulemaster.orderNumber = SalesOrders.orderNumber
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].ProductionLineInfo SchLine WITH (NOLOCK) ON schedulemaster.lineNumber = SchLine.lineInfoID
			RIGHT JOIN [CNFSQL04].Glass.soc.orderStats j
				ON j.sourceID = 1
					AND j.plantID = SalesOrders.plantRoutingID
					AND j.lineLinkID = COALESCE(schedulemaster.linenumber, ProductionLineInfo.lineInfoID, 0)
					AND j.orderNumber = SalesOrders.orderNumber
			WHERE j.sourceID = ?
				AND j.plantID = ?
				AND j.graphStart BETWEEN ? AND ?
			ORDER BY j.sourceID, j.plantID, j.lineNumber, j.orderNumber
	"""
	dsOut = system.db.runPrepQuery(qry, [ sourceID, plantID, startDate, endDate ], database)
	return system.dataset.toDataSet(dsOut)


def get_erp_rolls(database, sourceID, plantID, startDate, endDate):
	qry = """
	SELECT TOP (50000)
			1 AS sourceID
			,COALESCE(SalesOrders.plantRoutingID, j.plantID) AS plantID
			,COALESCE(schedulemaster.linenumber, ProductionLineInfo.lineInfoID, j.lineLinkID) AS lineLinkID
			,COALESCE(ProductionLineInfo.linenumber, schline.linenumber, j.lineNumber) AS lineNumber
			,SalesOrders.ordernumber AS orderNumber_sales
			,j.orderNumber
			,SalesOrders.qty AS orderQuantity
			,ScheduleMASTER.orderBalance
			,SalesOrders.jobStatus
			,ProductionData.prodTargetRollWeight AS targetRollWeight
			,WorkOrder.runRolls
			,WorkOrder.rollsAcross
			,ProductionData.prodStartDate
			,ProductionData.prodCompleteDate
			,SalesOrders.itemMasterID
			,ItemMaster.itemMasterNumber AS itemCode
			,SalesOrders.cnumber AS productCode
			,SalesOrders.WidthInInches AS targetWidth
			,SalesOrders.mil AS targetMil
			,schedulemaster.lbsHour AS erpTarget
			,ProductionLineInfo.targetRate AS defaultTarget
			,ProductionData.prodComplete
	FROM [CNFSQLProd01].[CharterSQL_RC].[dbo].[salesorders] WITH (NOLOCK)
			JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].[ItemMaster] WITH (NOLOCK) ON SalesOrders.itemMasterID = ItemMaster.itemMasterID
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].productiondata WITH (NOLOCK) ON SalesOrders.ordernumber = productiondata.ordernumber 
			INNER JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].WorkOrder WITH (NOLOCK) ON SalesOrders.orderNumber = WorkOrder.orderNumber 
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].ProductionLineInfo WITH (NOLOCK) ON ProductionLineInfo.lineInfoID = productiondata.prodLineID
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].schedulemaster WITH (NOLOCK) ON schedulemaster.orderNumber = SalesOrders.orderNumber
			LEFT JOIN [CNFSQLProd01].[CharterSQL_RC].[dbo].ProductionLineInfo SchLine WITH (NOLOCK) ON schedulemaster.lineNumber = SchLine.lineInfoID
			RIGHT JOIN [CNFSQL04].Glass.soc.orderStats j
				ON j.sourceID = 1
					AND j.plantID = SalesOrders.plantRoutingID
					AND j.lineLinkID = COALESCE(schedulemaster.linenumber, ProductionLineInfo.lineInfoID, 0)
					AND j.orderNumber = SalesOrders.orderNumber
			WHERE j.sourceID = ?
				AND j.plantID = ?
				AND j.graphStart BETWEEN ? AND ?
			ORDER BY j.sourceID, j.plantID, j.lineNumber, j.orderNumber
	"""
	dsOut = system.db.runPrepQuery(qry, [ sourceID, plantID, startDate, endDate ], database)
	return system.dataset.toDataSet(dsOut)


def getDowntimeEvents(database, sourceID, plantID, startDate, endDate):
	qry = """
	SELECT TOP(10000)
		a.*,
		b.ParentEventCode,
		b.[Name], 
		b.[Description], 
		b.IsDowntime, 
		b.IsPlanned,
		DATEDIFF(hour, a.StartTime, a.EndTime) AS 'Hours', 
		DATEDIFF(minute, a.StartTime, a.EndTime) % 60 AS 'Minutes',
		CONVERT(VARCHAR(5), DATEADD(SECOND,DATEDIFF(SECOND, a.StartTime, a.EndTime), 0), 108) AS 'hh:mm',
		CASE
			WHEN b.ParentEventCode = 0 THEN 'Running'
			WHEN b.ParentEventCode = 2 THEN 'Planned Downtime'
			WHEN b.ParentEventCode = 3 THEN 'Unplanned Downtime'
			ELSE 'Error in parent code'
		END AS [State]		
	FROM soc.DowntimeEvents a
	JOIN soc.DowntimeCodes b
		ON a.EventCode = b.EventCode
	WHERE sourceID = ?
		AND plantID = ?
		AND ((EndTime > ? and StartTime < ?)
		OR (EndTime IS NULL and StartTime < ?))
	ORDER BY StartTime ASC
	"""
	downtimeEvents = system.db.runPrepQuery(qry, [ sourceID, plantID, startDate, endDate, endDate ], database)
	return system.dataset.toDataSet(downtimeEvents)


def getTargetHistory(database, socID):
	qry = """
	DROP TABLE IF EXISTS #socTargets;
		SELECT TOP (1000) 
			h.[pk_socID] AS socID
		  ,h.[creationDate]
		  ,0 AS revisionLevel
		  ,NULL AS revisionDate
		  ,h.reviewer AS userName
		  ,NULL AS paramOldValue
		  ,NULL AS paramNewValue
		  ,d.paramValue AS currentTarget
		  INTO #socTargets
	  FROM [Glass].[soc].[Header] h
	  LEFT JOIN soc.[Data] d ON h.[pk_socID] = d.fk_socID
	  WHERE h.[pk_socID]  = {socID} AND d.fk_paramDefID = 40 
	UNION
		SELECT TOP (1000) 
			h.[pk_socID] AS socID
		  ,NULL AS creationDate
			  ,rh.[revisionLevel]
		  ,rh.[revisionDate]
		  ,rh.revisedBy AS userName
		  ,rd.[paramOldValue]  
		  ,rd.[paramNewValue]
		  ,d.paramValue AS currentTarget
	  FROM [Glass].[soc].[Header] h
	  LEFT JOIN soc.[Data] d ON h.[pk_socID] = d.fk_socID
	  LEFT JOIN [soc].[revisionHeader] rh ON rh.fk_socID = h.pk_socID
	  LEFT JOIN [Glass].[soc].[revisionData] rd ON rh.[pk_revisionID] = rd.[fk_revisionID]
	  WHERE h.[pk_socID]  = {socID} AND d.fk_paramDefID = 40 AND rd.fk_paramDefID = 40
	  ORDER BY revisionLevel

	 SELECT 
		socID
		,revisionLevel AS revLevel
		,CASE
			WHEN (SELECT COUNT(*) FROM #socTargets) = 1 THEN currentTarget
			WHEN revisionLevel + 1 = (SELECT COUNT(*) FROM #socTargets) THEN paramNewValue
			ELSE 		LEAD(paramOldValue,1,paramNewValue) OVER ( --PARTITION BY rh.[fk_socID], rh.[revisionLevel]
				ORDER BY revisionLevel)
		END AS socTarget
		,CASE
			WHEN revisionDate IS NULL THEN creationDate
			ELSE revisionDate
		END AS effectiveDate
		,userName
	FROM #socTargets
	DROP TABLE IF EXISTS #socTargets
	""".format(socID = socID)
	socTargets = system.db.runQuery(qry, database)
	return system.dataset.toDataSet(socTargets)