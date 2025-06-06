# OEE Vision Application

## Data Gathering

#### Declarations
```sql
DECLARE @sourceID INT
DECLARE @plantID INT
DECLARE @lineNumber INT
DECLARE @lineLinkID INT
DECLARE @orderNumber INT
DECLARE @startDate DATETIME
DECLARE @endDate DATETIME
```
### Example settings
```sql
SET @sourceID = 1
SET @plantID = 1
SET @lineNumber = 4
SET @lineLinkID = 4
SET @orderNumber = 467525
SET @startDate = '4-9-2025'
SET @endDate = '4-16-2025'
```


#### Get distinct orderNumbers for plant from CNFSQL04
```sql
/* Get orderNumbers within date range */
SELECT DISTINCT
	[sourceID]
	,[plantID]
	,[lineLinkID]
	,[lineNumber]
	,[orderNumber]
INTO #orderNumbers
FROM [CNFSQL04].Glass.soc.orderStats
WHERE sourceID = @sourceID
	AND plantID = @plantID
	AND graphStart BETWEEN @startDate AND @endDate 
```

#### Get roll detail from  CharterSQL_RC
```sql
/* Get Production Items */
SELECT TOP(100000)
    *  /* Specify columns */ 
INTO #prodItems 
FROM [CNFSQLProd01].[CharterSQL_RC].[dbo].productionitems 
WHERE orderNumber IN (SELECT orderNumber FROM #orderNumbers)
```

#### Process roll data
#### Do this in python instead of sql
```sql
SELECT
	(lbs_produced - COALESCE( (SELECT SUM( COALESCE( [weight], 0) ) 
								FROM #prodItems
								WHERE orderNumber = o.orderNumber 
									AND orderNumber != 0 
									AND prodLineID = o.lineLinkID
									AND ( [pitStartTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
											OR [pitEndTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
											OR [prodDateStamp] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd) )
							), -1 )) / lbs_produced AS pctTrim
	,COALESCE( (SELECT SUM( COALESCE( [weight], 0) ) 
				FROM #prodItems
				WHERE orderNumber = o.orderNumber 
					AND orderNumber != 0 
					AND prodLineID = o.lineLinkID
					AND ( [pitStartTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
							OR [pitEndTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
							OR [prodDateStamp] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd) )
				), -1 ) AS totalWeight
	,COALESCE( (SELECT SUM(CASE 
								WHEN itemScrapped = 1 
								THEN COALESCE( [weight] , 0)
								ELSE 0 
									END) 
				FROM #prodItems 
				WHERE orderNumber = o.orderNumber 
						AND orderNumber != 0 
						AND prodLineID = o.lineLinkID
						AND ( [pitStartTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
								OR [pitEndTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
								OR [prodDateStamp] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd) )
				),-1 ) AS scrapWeight
	,COALESCE( (SELECT COUNT([weight]) 
								FROM #prodItems
					WHERE orderNumber = o.orderNumber 
						AND orderNumber != 0 
						AND prodLineID = o.lineLinkID
						AND ( [pitStartTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
								OR [pitEndTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
								OR [prodDateStamp] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd) )
				), -1 ) AS rollCount
	,COALESCE( (SELECT COUNT([weight]) 
				FROM #prodItems
				WHERE orderNumber = o.orderNumber 
					AND orderNumber != 0 
					AND prodLineID = o.lineLinkID
					AND itemScrapped = 1 
					AND ( [pitStartTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
							OR [pitEndTime] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd)
							OR [prodDateStamp] BETWEEN DATEADD(hour, -24, o.graphStart) AND DATEADD(hour, 24, o.graphEnd) )
				), -1 ) AS scrapCount
		,*
FROM [CNFSQL04].Glass.soc.orderStats o WHERE o.sourceID = 1 AND o.orderNumber IN (SELECT orderNumber FROM #orderNumbers)
ORDER BY orderNumber
```

#### Cleanup
```sql
DROP TABLE IF EXISTS #orderNumbers
DROP TABLE IF EXISTS #prodItems
```