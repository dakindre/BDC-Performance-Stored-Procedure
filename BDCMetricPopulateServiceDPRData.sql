if object_id('tempdb..#DPRSRVDealerPerformanceData') is not null drop table #DPRSRVDealerPerformanceData
select bac
	, CASE WHEN metrics = 'Leads Analyzed' AND leadTypeID = 3 THEN 10
		WHEN metrics = 'Leads Analyzed' AND leadTypeID = 1 THEN 11
		WHEN metrics = 'Leads Analyzed' AND leadTypeID = 2 THEN 12
		WHEN metrics = 'Close %' AND leadTypeID = 2 THEN 29
		WHEN metrics = 'Close %' AND leadTypeID = 3 THEN 30
		WHEN metrics = 'Close %' AND leadTypeID = 1 THEN 31
		WHEN metrics = 'Avg. Response Time' AND leadTypeID = 3 THEN 32
		WHEN metrics = 'Avg. Response Time' AND leadTypeID = 1 THEN 33
		END AS metricNameID
	, CASE competitiveAvgTypeID WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 5 THEN 3 WHEN 6 THEN 4 END AS marketCompID 
	, monthlyDealer, monthlyCompetitive
INTO #DPRSRVDealerPerformanceData
from dbo.DPRSRVDealerPerformanceData
where rollingMonthAvgID = (SELECT rollingMonthAvgID FROM DPRSRVDealerRollingMonthAvg WHERE isThisDefaultSelection = 1)
AND competitiveAvgTypeID in (1,2,5,6)
AND ((metrics = 'Leads Analyzed' AND leadTypeID = 3)
	OR (metrics = 'Leads Analyzed' AND leadTypeID = 1)
	OR (metrics = 'Leads Analyzed' AND leadTypeID = 2)
	OR (metrics = 'Close %' AND leadTypeID = 2 )
	OR (metrics = 'Close %' AND leadTypeID = 3)
	OR (metrics = 'Close %' AND leadTypeID = 1)
	OR (metrics = 'Avg. Response Time' AND leadTypeID = 3)
	OR (metrics = 'Avg. Response Time' AND leadTypeID = 1)
)

if object_id('tempdb..#DealerOSSTool') is not null drop table #DealerOSSTool
SELECT bac, REPLACE(region,' REGION', '') as region, zone, district
	, CASE Color WHEN 'Green' THEN 'Y' ELSE 'N' END AS ossTool
INTO #DealerOSSTool
--SELECT *
FROM dbo.DPRSRVDealerPerformanceHeaderData h
LEFT JOIN (
	SELECT DISTINCT DPRPopulation, Color
	FROM dbo.ServiceOSSLeadsMatrix
)m
ON h.OSStool = m.DPRPopulation


/**************************
Update DART Tables
***************************/

--Update Dealer Header Info
UPDATE d
SET ossTools = ISNULL(oss.ossTool,'N'),
	serviceCloseRateRnk = closeRank
FROM dbo.BDCMetricDealerInfo d
LEFT JOIN #DealerOSSTool oss ON d.bac = oss.bac
LEFT JOIN (
	SELECT bac,closeRank FROM dbo.DPRSRVDealerPerformanceData
	WHERE rollingMonthAvgID = (SELECT rollingMonthAvgID FROM DPRSRVDealerRollingMonthAvg WHERE isThisDefaultSelection = 1)
	AND competitiveAvgTypeID = 6
	AND metrics = 'Close %'
	AND leadTypeID = 4
)r ON r.bac = d.bac


--OSS Field Rollup with BDCMetricDealerInfo
if object_id('tempdb..#FieldOSSTool') is not null drop table #FieldOSSTool
select unpvt.region, serviceZone, serviceDistrict, fieldCompAvgID
	, CASE metric WHEN 'OSSTool' THEN 13
		END AS metric
	, value
INTO #FieldOSSTool
FROM (
	SELECT ISNULL(region,'NATIONAL') as region, zone as serviceZone, serviceDistrict
		, case when region IS NULL then '1'
			when zone IS NULL then '2'
			when serviceDistrict IS NULL then '3'
			else '4'
			end as fieldCompAvgID
		, ROUND(dbo.Divide_Decimal(COUNT(CASE WHEN ossTools = 'Y' THEN ossTools END), COUNT(DISTINCT bac)),3)*100 as OSSTool
	FROM dbo.BDCMetricDealerInfo d
	LEFT JOIN (
		SELECT DISTINCT zone, district
		FROM dbo.DPRSRVDealerPerformanceHeaderData
	)m
	ON d.serviceDistrict = m.district
	GROUP BY GROUPING sets (
		(region, zone, serviceDistrict),
		(region, zone),
		(region),
		()
	)
)p
unpivot (value for metric IN (OSSTool))unpvt

--SELECT * FROM #FieldOSSTool



DELETE FROM dbo.BDCMetricMetrics WHERE metricNameID IN (10,11,12,13,29,30,31,32,33)


if object_id('tempdb..#DealerInfo') is not null drop table #DealerInfo
select bac, region, m.zone as serviceZone, serviceDistrict, metricNameID
INTO #DealerInfo
from dbo.BDCMetricDealerInfo d
LEFT JOIN (
	SELECT DISTINCT zone, district
	FROM dbo.DPRSRVDealerPerformanceHeaderData
)m
ON d.serviceDistrict = m.district
cross join (
	select metricNameID 
	from dbo.BDCMetricMetricName
	WHERE metricNameID IN (10,11,12,13,29,30,31,32,33)
) metric


--Import DPRSRV data into BDCMetric
INSERT INTO dbo.BDCMetricMetrics(bac, metricNameID, marketCompID, value, valueRGB, marketCompAvg, timePeriod)
SELECT d.bac, d.metricNameID, c.marketCompID
	,CASE WHEN d.metricNameID IN(
			10/*Website Lead Volume*/
			,11/*DMN Lead Volume*/
			,12/*Dealer Web Phone Calls*/) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, s.monthlyDealer) AS MONEY), 1), '.00', ''),'-')
		WHEN d.metricNameID IN(
			29/*Website Phone Call Close Rate*/
			,30/*Website Close Rate*/
			,31/*DMN Close Rate*/) THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1),  s.monthlyDealer*100.000))+'%'), '-')	
		WHEN d.metricNameID IN(32/*Website Average Response Time*/,33/*DMN Average Response Time*/) THEN s.monthlyDealer
		--ELSE CONVERT(VARCHAR(20), CONVERT(DECIMAL(20,10), s.monthlyDealer))
		END AS value
	, CASE WHEN d.metricNameID IN(10, 12, 29, 30, 31) AND CONVERT(DECIMAL(20,10), s.monthlyDealer) < CONVERT(DECIMAL(20,10), s.monthlyCompetitive) THEN '244,186,186' 
		WHEN d.metricNameID IN(10, 12, 29, 30, 31) AND CONVERT(DECIMAL(20,10), s.monthlyDealer) >= CONVERT(DECIMAL(20,10), s.monthlyCompetitive) THEN '226,240,217'
		WHEN d.metricNameID IN(32, 33) AND dbo.ConvertResponseTimeToDays(s.monthlyDealer) >= dbo.ConvertResponseTimeToDays(s.monthlyCompetitive) THEN '244,186,186' 
		WHEN d.metricNameID IN(32, 33) AND dbo.ConvertResponseTimeToDays(s.monthlyDealer) < dbo.ConvertResponseTimeToDays(s.monthlyCompetitive) THEN '226,240,217'
		ELSE '255,255,255' 
		END AS valueRGB
	, CASE WHEN d.metricNameID IN(
		10/*Website Lead Volume*/
		,11/*DMN Lead Volume*/
		,12/*Dealer Web Phone Calls*/) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, s.monthlyCompetitive) AS MONEY), 1), '.00', ''),'-')
		WHEN d.metricNameID IN(
			29/*Website Phone Call Close Rate*/
			,30/*Website Close Rate*/
			,31/*DMN Close Rate*/) THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), s.monthlyCompetitive*100.000))+'%'), '-')		
		WHEN d.metricNameID IN(32/*Website Average Response Time*/,33/*DMN Average Response Time*/) THEN s.monthlyCompetitive
		--ELSE CONVERT(VARCHAR(20), s.monthlyCompetitive)
		END AS marketCompAvg
	, tp.timePeriod
FROM #DealerInfo d
CROSS JOIN dbo.BDCMetricMarketComparison c
LEFT JOIN #DPRSRVDealerPerformanceData s
ON d.bac = s.bac AND d.metricNameID = s.metricNameID AND s.marketCompID = c.marketCompID
CROSS JOIN (SELECT MAX(timePeriod) as timePeriod from dbo.BDCMetricMetrics) tp
WHERE d.metricNameID IN (10,11,12,29,30,31,32,33)

--District OSS Tool
INSERT INTO dbo.BDCMetricMetrics(bac, metricNameID, marketCompID, value, valueRGB, marketCompAvg, timePeriod)
SELECT d.bac, d.metricNameID, field.fieldCompAvgID
	, ISNULL(deal.ossTool,'N') as value
	, CASE WHEN deal.ossTool = 'Y' THEN '226,240,217' 
		ELSE '244,186,186'  
		END AS valueRGB
	, CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), field.value))+'%' AS marketCompAvg
	, tp.timePeriod
FROM #DealerInfo d
LEFT JOIN #DealerOSSTool deal
ON d.bac = deal.bac
	AND d.region = deal.region 
	AND d.serviceZone = deal.zone
	AND d.serviceDistrict = deal.district
LEFT JOIN #FieldOSSTool field 
ON d.region = field.region
	AND d.serviceZone = field.serviceZone 
	AND d.serviceDistrict = field.serviceDistrict 
CROSS JOIN (SELECT MAX(timePeriod) as timePeriod from dbo.BDCMetricMetrics) tp
WHERE d.metricNameID = 13


--Zone
INSERT INTO dbo.BDCMetricMetrics(bac, metricNameID, marketCompID, value, valueRGB, marketCompAvg, timePeriod)
SELECT d.bac, d.metricNameID, field.fieldCompAvgID
	, ISNULL(deal.ossTool,'N') as value
	, CASE WHEN deal.ossTool = 'Y' THEN '226,240,217' 
		ELSE '244,186,186'  
		END AS valueRGB
	, CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), field.value))+'%' AS marketCompAvg
	, tp.timePeriod
FROM #DealerInfo d
LEFT JOIN #DealerOSSTool deal
ON d.bac = deal.bac
	AND d.region = deal.region 
	AND d.serviceZone = deal.zone
	AND d.serviceDistrict = deal.district
LEFT JOIN #FieldOSSTool field 
ON d.region = field.region
	AND d.serviceZone = field.serviceZone 
CROSS JOIN (SELECT MAX(timePeriod) as timePeriod from dbo.BDCMetricMetrics) tp
WHERE d.metricNameID = 13 AND field.serviceDistrict IS NULL


--Region
INSERT INTO dbo.BDCMetricMetrics(bac, metricNameID, marketCompID, value, valueRGB, marketCompAvg, timePeriod)
SELECT d.bac, d.metricNameID, field.fieldCompAvgID
	, ISNULL(deal.ossTool,'N') as value
	, CASE WHEN deal.ossTool = 'Y' THEN '226,240,217'
		ELSE '244,186,186'
		END AS valueRGB
	, CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), field.value))+'%' AS marketCompAvg
	, tp.timePeriod
FROM #DealerInfo d
LEFT JOIN #DealerOSSTool deal
ON d.bac = deal.bac
	AND d.region = deal.region
	AND d.serviceZone = deal.zone
	AND d.serviceDistrict = deal.district
LEFT JOIN #FieldOSSTool field
ON d.region = field.region
CROSS JOIN (SELECT MAX(timePeriod) as timePeriod from dbo.BDCMetricMetrics) tp
WHERE d.metricNameID = 13 AND field.serviceZone IS NULL AND field.serviceDistrict IS NULL AND field.region != 'NATIONAL'



--National
INSERT INTO dbo.BDCMetricMetrics(bac, metricNameID, marketCompID, value, valueRGB, marketCompAvg, timePeriod)
SELECT d.bac, d.metricNameID, field.fieldCompAvgID
	, ISNULL(deal.ossTool,'N') as value
	, CASE WHEN deal.ossTool = 'Y' THEN '226,240,217' 
		ELSE '244,186,186'  
		END AS valueRGB
	, CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), field.value))+'%' AS marketCompAvg
	, tp.timePeriod
FROM #DealerInfo d
LEFT JOIN #DealerOSSTool deal
ON d.bac = deal.bac
	AND d.region = deal.region 
	AND d.serviceZone = deal.zone
	AND d.serviceDistrict = deal.district
LEFT JOIN #FieldOSSTool field 
ON field.region = 'NATIONAL'
CROSS JOIN (SELECT MAX(timePeriod) as timePeriod from dbo.BDCMetricMetrics) tp
WHERE d.metricNameID = 13


/**************************
Footnotes
***************************/
--SELECT * FROM dbo.BDCMetricFootnote

if object_id('tempdb..#DataSources') is not null drop table #DataSources
create table #DataSources
(
  metric varchar(100) NULL,
  dataSource varchar(100) NULL,
  reportPeriod date NULL,
  orderID int NULL
)

insert into #DataSources select 'Service Leads','Acxiom', MAX(datePeriod), 7 from dbo.DPRSRVDealerPerformanceData
insert into #DataSources select 'Online Service Scheduling (OSS)','GM', MAX(reportPeriod), 8 from dbo.ServiceOSSLeads

DELETE FROM dbo.BDCMetricFootnote WHERE footnoteName IN ('Service Leads','Online Service Scheduling')

INSERT INTO dbo.BDCMetricFootnote (footnoteType, footnoteName, footnoteText, footnoteDate)
select 
	'Footer'
	, metric
	, metric + ' Data Source: ' + dataSource + '  '
	, DATENAME(MONTH, reportPeriod) + ' ' + convert(varchar(4),YEAR(reportPeriod))
from #DataSources



/**************************
Resources
***************************/

--Need to modify mappings if new metrics are added
if object_id('tempdb..#ResourceMetricMap') is not null drop table #ResourceMetricMap
select 1 AS resourceLinkID, 4 AS metricNameID
into #ResourceMetricMap
union select 2,6
union select 3,8
union select 4,10
union select 5,12
union select 6,17
union select 7,22
union select 8,23
union select 9,24
union select 10,26
union select 11,29
union select 12,30
union select 13,31
union select 14,35
union select 15,36




if object_id('tempdb..#ResourceMetricCombined') is not null drop table #ResourceMetricCombined
select m.metricNameID, m.metricName, r.resourceLinkID, r.resourceHTML 
into #ResourceMetricCombined
from dbo.BDCMetricMetricName m
join #ResourceMetricMap mr
on m.metricNameID = mr.metricNameID
join dbo.BDCMetricResourceLinks r
on mr.resourceLinkID = r.resourceLinkID



DELETE FROM dbo.BDCMetricResources WHERE resourceLinkID IN (SELECT resourceLinkID FROM #ResourceMetricCombined WHERE metricNameID IN (10,12,29,30,31))

if object_id('tempdb..#BDCMetricMetricsService') is not null drop table #BDCMetricMetricsService
SELECT bac, metricNameID, marketCompID
	, CASE WHEN metricNameID IN (10,12) THEN value
		WHEN metricNameID IN (29,30,31) THEN CONVERT(VARCHAR(20), REPLACE(value,'%',''))
		END AS value
	, CASE WHEN metricNameID IN (10) THEN marketCompAvg
		WHEN metricNameID IN (29,30,31) THEN CONVERT(VARCHAR(20), REPLACE(marketCompAvg,'%',''))
		END AS marketCompAvg
INTO #BDCMetricMetricsService
FROM dbo.BDCMetricMetrics
WHERE metricNameID IN (10,12,29,30,31)

--Populate Resources
INSERT INTO dbo.BDCMetricResources (resourceLinkID, marketCompID, bac)
SELECT r.resourceLinkID, m.marketCompID, m.bac
FROM #BDCMetricMetricsService m
JOIN #ResourceMetricCombined r
ON m.metricNameID = r.metricNameID
WHERE ((m.metricNameID IN (10,12,29,30,31) AND m.value < m.marketCompAvg AND m.value <> '-')
)

--Populate orderID
UPDATE r
SET orderID = orderIDUpdate
FROM dbo.BDCMetricResources r
JOIN (
	SELECT *, row_number()over(partition by bac, marketCompID order by resourceLinkID) as orderIDUpdate
	FROM dbo.BDCMetricResources
) rr
ON r.resourceID = rr.resourceID



/***********************************
 Update Metric ToolTips With Date
***********************************/

if object_id('tempdb..#MetricToolTip') is not null drop table #MetricToolTip
SELECT t.metricNameID, t.metricName, t.metricTooltip
INTO #MetricToolTip
FROM dbo.BDCMetricMetricTooltip t
JOIN dbo.BDCMetricMetricName n ON t.metricNameID = n.metricNameID
					
				
--Website Lead Volume
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 10) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Service Leads'))
WHERE metricNameID = 10

--DMN Lead Volume
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 11) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Service Leads'))
WHERE metricNameID = 11

--Dealer Web Phone Calls
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 12) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Service Leads'))
WHERE metricNameID = 12

--Website Phone Call Close Rate
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 29) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Service Leads'))
WHERE metricNameID = 29

--Website Close Rate
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 30) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Service Leads'))
WHERE metricNameID = 30

--DMN Close Rate
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 31) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Service Leads'))
WHERE metricNameID = 31

--Website Average Response Time
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 32) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Service Leads'))
WHERE metricNameID = 32

--DMN Average Response Time
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 33) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Service Leads'))
WHERE metricNameID = 33

--OSS
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 13) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Online Service Scheduling (OSS)'))
WHERE metricNameID = 13