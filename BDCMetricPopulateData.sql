declare @reportPeriod as date = '2017-12-31'

/**************************
Dealer List Temp Table
***************************/

if object_id('tempdb..#DealerList') is not null drop table #DealerList

;with mfc as (
	select bac, gmLines
	from GM_Dynamic.dbo.MasterfileDailyByRooftopView mf
	where openPointFlag = 'N'
)
SELECT right(mf.bac,6) as bac, isnull(dealerDBAName,dealerIncorporatedName) as dealerName, mfc.gmLines
	, case when mfc.bac is not null then 1 end as active
	, r.regionShort, replace(mf.regionName,' REGION','') as region
	, zoneName as slsZone, districtName as slsDistrict, mf.districtManagerName as slsManager
	, aftersalesZoneName as svcZone, aftersalesDistrictName as svcDistrict, mf.aftersalesdistrictManagerName as svcManager
	, rank()over(partition by mf.bac order by reportPeriod desc, active desc) as mfRnk
into #DealerList
FROM GM_Dynamic.dbo.MasterfileDailyHistory mf
left join mfc
on mf.bac = mfc.bac
left join GM_Static..Regions r
on mf.regionName = r.regionName
where mf.openPointFlag = 'N'
and mf.unassignedFlag = 'N'
and BMD = DIVISION

DELETE FROM #DealerList WHERE mfRnk <> 1


/**************************
Equity Mining
***************************/
if object_id('tempdb..#EMVendorTemp') is not null drop table #EMVendorTemp
SELECT 
	BAC, emVendor as Vendor,  ROW_NUMBER() OVER(Partition by BAC ORDER BY emAttributedSales DESC) as rank
INTO #EMVendorTemp
FROM DETGMUSCRM.dbo.EquityMiningUsage AS a
WHERE reportPeriod = (SELECT MAX(reportPeriod)FROM DETGMUSCRM.dbo.EquityMiningUsage)


if object_id('tempdb..#EquityMining') is not null drop table #EquityMining

SELECT 
	e.reportPeriod
	,e.BAC
	,(SELECT Vendor FROM #EMVendorTemp v WHERE v.BAC = e.BAC AND rank = 1) AS Vendor
	,SUM(emAttributedSales) AS emAttributedSales
	,1 AS value
	,MAX(retailSales) AS retailSales
INTO #EquityMining
FROM DETGMUSCRM.dbo.EquityMiningUsage e
WHERE e.reportPeriod = (SELECT MAX(reportPeriod)FROM DETGMUSCRM.dbo.EquityMiningUsage)
AND e.BAC IS NOT NULL
GROUP BY e.BAC, retailSales, e.reportPeriod


/**************************
Playbook Metrics
***************************/

if object_id('tempdb..#Playbook') is not null drop table #Playbook

SELECT reportPeriod, bac, inMarketFavorGM, inMarketFavorUndecided, newCarCountHigh, myCarCountHigh, oldCarCountHigh, aCarCountHigh 
INTO #Playbook
FROM dbo.PlaybookCompass
WHERE reportPeriod = (SELECT MAX(reportPeriod) FROM dbo.PlaybookCompass)

if object_id('tempdb..#PlaybookCampaign') is not null drop table #PlaybookCampaign

SELECT reportPeriod, bac, '1' AS Flag, SUM(TotalVehiclesSold) AS TotalVehiclesSold, SUM(CustomersContacted) AS CustomersContacted
INTO #PlaybookCampaign
FROM dbo.PlaybookCampaigns 
WHERE reportPeriod = (SELECT MAX(reportPeriod) FROM dbo.PlaybookCampaigns)
AND CampaignEndDate <= @reportPeriod
GROUP BY reportPeriod, bac

/**************************
MPVI
***************************/
if object_id('tempdb..#MPVI') is not null drop table #MPVI

SELECT d.reportPeriod
, D.BAC_CODE as bac
, isnull(R.CP_WARR_RO,0) as CPWARRRO
, isnull(W.MPVI_NBR_PERFORMED,0) as mpviPerformed
, isnull(W.MENU_NBR_PRESENTED,0) as menuPresented
INTO #MPVI
FROM DETGMUSCRM.dbo.ServiceDealers D
LEFT JOIN ServiceROs R ON D.BAC_CODE = R.BAC_CODE 
AND R.reportPeriod = D.reportPeriod
LEFT JOIN DETGMUSCRM.dbo.ServiceMPVIMenu W
ON D.BAC_CODE = W.BAC_CODE
AND D.REPORTPERIOD = W.REPORTPERIOD 
WHERE D.reportPeriod = (SELECT MAX(reportPeriod) FROM DETGMUSCRM.dbo.ServiceDealers)

UPDATE d 
SET d.active = 0
-- select * 
FROM #DealerList d
where active is null
AND EXISTS(
	SELECT * 
	FROM #MPVI l
	WHERE not(isnull(CPWARRRO,0) = 0 and isnull(mpviPerformed,0) = 0 and isnull(menuPresented,0) = 0)
	and d.bac = l.bac
)


/**************************
Selling During Service Visit
***************************/

if object_id('tempdb..#SDSV') is not null drop table #SDSV

SELECT reportPeriod, RIGHT(bac, 6) AS bac, uniqueHouseholds, salesAttributed
INTO #SDSV
FROM DETGMUSCRM.dbo.SDSVPerformance
WHERE reportPeriod = (SELECT MAX(reportPeriod)FROM DETGMUSCRM.dbo.SDSVPerformance)


/**************************
Procare Advocate
***************************/
if object_id('tempdb..#ProcareAdvocate') is not null drop table #ProcareAdvocate
SELECT bac, max(advocateName) as advocateContactInfo
INTO #ProcareAdvocate
FROM dbo.AdvocateAssignments
WHERE reportPeriod = (SELECT MAX(reportPeriod) FROM dbo.AdvocateAssignments)
GROUP BY bac


/**************************
Digital District Manager
***************************/
if object_id('tempdb..#DDM') is not null drop table #DDM
SELECT bac, nameFirst + ' ' + nameLast AS DDMName, jobName, districtName, slsDistrict
INTO #DDM
FROM #DealerList d
LEFT JOIN GM_Dynamic.dbo.EmployeeProfile e ON e.districtName = d.slsDistrict
WHERE jobName LIKE 'District Digital%'


/**************************
Dealer Sales Loyalty
***************************/

if object_id('tempdb..#DealerSalesLoyalty') is not null drop table #DealerSalesLoyalty

SELECT bac, timePeriod, SUM(brandLoyalDealerLoyalRolling3Month) AS brandLoyalDealerLoyalRolling3Month, SUM(returnToMarketRolling3Month) AS returnToMarketRolling3Month
INTO #DealerSalesLoyalty
FROM GM_Dynamic.dbo.Dealer_Sales_Loyalty
WHERE timePeriod = (SELECT MAX(timePeriod)FROM GM_Dynamic.dbo.Dealer_Sales_Loyalty)
AND bac IS NOT NULL
GROUP BY bac, timePeriod

/**************************
EBE
***************************/
if object_id('tempdb..#EBE') is not null drop table #EBE

SELECT	reportPeriod
		,bac
		,CASE WHEN enrolledEBE IS NOT NULL THEN 'Y'
		ELSE 'N'
		END AS enrolledEBE
INTO #EBE
FROM dbo.Enrollment
WHERE reportPeriod =  (SELECT MAX(reportPeriod) FROM dbo.Enrollment)


/**************************
Master Table Full
***************************/
if object_id('tempdb..#DealerTableFull') is not null drop table #DealerTableFull

SELECT
	RIGHT(deal.bac, 6) AS bac
	,deal.active
	,deal.dealerName
	,deal.region
	,deal.slsZone
	,deal.slsDistrict
	,deal.svcDistrict
	,deal.svcZone
	,deal.regionShort
	,deal.slsManager
	,deal.svcManager 
	,deal.gmLines AS divisions
	,equity.value AS EquityMiningParticipation
	,equity.Vendor
	,equity.emAttributedSales
	,equity.retailSales
	,play.inMarketFavorGM
	,play.inMarketFavorUndecided
	,play.newCarCountHigh
	,play.myCarCountHigh
	,play.oldCarCountHigh
	,play.aCarCountHigh
	,camp.Flag
	,camp.TotalVehiclesSold
	,camp.CustomersContacted
	,MPVI.mpviPerformed AS MPVIPerformed
	,MPVI.CPWARRRO AS CPWARRRO
	,SDSV.salesAttributed AS SDSVSalesAttributed
	,SDSV.uniqueHouseholds AS SDSVUniqueHouseholds
	,LOYAL.brandLoyalDealerLoyalRolling3Month 
	,LOYAL.returnToMarketRolling3Month
	,ebe.enrolledEBE
	,pro.advocateContactInfo AS proCareAdvocate
	,ddm.DDMName AS DDMName
INTO #DealerTableFull
FROM #DealerList deal
	LEFT JOIN #EquityMining equity ON equity.BAC = deal.bac
	LEFT JOIN #MPVI MPVI ON MPVI.bac = deal.bac
	LEFT JOIN #SDSV SDSV ON SDSV.bac = deal.bac
	LEFT JOIN #DealerSalesLoyalty LOYAL ON LOYAL.bac = deal.bac
	LEFT JOIN #EBE ebe ON ebe.bac = deal.bac
	LEFT JOIN #Playbook play ON play.bac = deal.bac
	LEFT JOIN #PlaybookCampaign camp ON camp.BAC = deal.bac
	LEFT JOIN #ProcareAdvocate pro ON pro.bac = deal.bac
	LEFT JOIN #DDM ddm ON ddm.bac = deal.bac
WHERE active IN (1,0)

/**************************
Data Sources (for Footnotes)
***************************/

if object_id('tempdb..#DataSources') is not null drop table #DataSources
create table #DataSources
(
  metric varchar(100) NULL,
  dataSource varchar(100) NULL,
  reportPeriod date NULL,
  orderID int NULL
)

insert into #DataSources select 'Equity Mining', 'GM', MAX(reportPeriod), 3 from #EquityMining
insert into #DataSources select 'MPVI','Service Workbench', MAX(reportPeriod), 4 from #MPVI
insert into #DataSources select 'SDSV','GM Exchange/Integralink, CDR, TSD', MAX(reportPeriod), 5 from #SDSV
insert into #DataSources select 'Dealer Sales Loyalty','IHS', MAX(timePeriod), 9 from #DealerSalesLoyalty
insert into #DataSources select 'EBE','Maritz', MAX(reportPeriod), 11 from #EBE
insert into #DataSources select 'Playbook', 'Epsilon', MAX(reportPeriod), 13 from #PlaybookCampaign

/**************************
Calculate metrics at dealer level
***************************/


if object_id('tempdb..#DealerData') is not null drop table #DealerData
select bac
		,region
		,slsZone
		,slsDistrict
		,CASE metric 
					WHEN 'EquityMiningParticipation' THEN 8
					WHEN 'PlaybookInMarketCustSales' THEN 9
					WHEN 'PlaybookInMarketCustService' THEN 14
					WHEN 'MPVIPenetration' THEN 15
					WHEN 'SDSVNewVehicleSalesCloseRate' THEN 25
					WHEN 'DealerSalesLoyalty' THEN 26
					WHEN 'EquityMiningShareSales' THEN 27
					WHEN 'PlaybookBuyRate' THEN 28
					WHEN 'VehicleSalesFromROS' THEN 34
					END AS metric	
									
	, CASE metric	
					WHEN 'EquityMiningParticipation' THEN EquityMiningParticipation
					WHEN 'PlaybookInMarketCustSales' THEN PlaybookInMarketCustSales
					WHEN 'PlaybookInMarketCustService' THEN PlaybookInMarketCustService
					WHEN 'MPVIPenetration' THEN MPVIPenetration
					WHEN 'SDSVNewVehicleSalesCloseRate' THEN SDSVNewVehicleSalesCloseRate
					WHEN 'DealerSalesLoyalty' THEN DealerSalesLoyalty
					WHEN 'EquityMiningShareSales' THEN EquityMiningShareSales
					WHEN 'PlaybookBuyRate' THEN PlaybookBuyRate
					WHEN 'VehicleSalesFromROS' THEN VehicleSalesFromROS
					END AS value
					
INTO #DealerData
FROM (
	SELECT 
		bac
		,region
		,slsZone
		,slsDistrict
		,CONVERT(DECIMAL(20,10), EquityMiningParticipation) AS EquityMiningParticipation
		,CONVERT(DECIMAL(20,10), inMarketFavorGM+inMarketFavorUndecided) AS PlaybookInMarketCustSales 
		,CONVERT(DECIMAL(20,10), newCarCountHigh+myCarCountHigh+oldCarCountHigh+aCarCountHigh) AS PlaybookInMarketCustService
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(MPVIPerformed, CPWARRRO),3)*100) AS MPVIPenetration
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(SDSVSalesAttributed, SDSVUniqueHouseholds),3)*100) AS SDSVNewVehicleSalesCloseRate
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(brandLoyalDealerLoyalRolling3Month, returnToMarketRolling3Month),3)*100) AS DealerSalesLoyalty
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(emAttributedSales, retailSales),3)*100) AS EquityMiningShareSales
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(TotalVehiclesSold, CustomersContacted),3)*100)  AS PlaybookBuyRate
		,CONVERT(DECIMAL(20,10), SDSVSalesAttributed) AS VehicleSalesFromROS
	FROM #DealerTableFull
	)p
CROSS JOIN (
	SELECT 'EquityMiningParticipation'
	UNION SELECT 'PlaybookInMarketCustSales' 
	UNION SELECT 'PlaybookInMarketCustService' 
	UNION SELECT 'MPVIPenetration' 
	UNION SELECT 'SDSVNewVehicleSalesCloseRate'
	UNION SELECT 'DealerSalesLoyalty'
	UNION SELECT 'EquityMiningShareSales' 
	UNION SELECT 'PlaybookBuyRate'
	UNION SELECT 'VehicleSalesFromROS'
	) unpvt(metric)



/*****************************************************
Combine Service Field Metrics with Sales Field Metrics
*****************************************************/
if object_id('tempdb..#FieldData') is not null drop table #FieldData


SELECT	unpvt.region
		,slsZone
		,slsDistrict
		,fieldCompAvgID
				,CASE metric 
					WHEN 'EquityMiningParticipation' THEN 8
					WHEN 'PlaybookInMarketCustSales' THEN 9
					WHEN 'PlaybookInMarketCustService' THEN 14
					WHEN 'MPVIPenetration' THEN 15
					WHEN 'SDSVNewVehicleSalesCloseRate' THEN 25
					WHEN 'DealerSalesLoyalty' THEN 26
					WHEN 'EquityMiningShareSales' THEN 27
					WHEN 'PlaybookBuyRate' THEN 28
					WHEN 'VehicleSalesFromROS' THEN 34
					END AS metric			
			, value
INTO #FieldData
FROM (
	SELECT ISNULL(region,'NATIONAL') as region, slsZone, slsDistrict,
		case when region IS NULL then '1' 
			when slsZone IS NULL then '2' 
			when slsDistrict IS NULL then '3' 
			else '4' end as fieldCompAvgID
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(COUNT(CASE WHEN EquityMiningParticipation = '1' THEN EquityMiningParticipation END), COUNT(BAC)),3)*100) AS EquityMiningParticipation
		,CONVERT(DECIMAL(20,10), AVG(inMarketFavorGM + inMarketFavorUndecided)) AS PlaybookInMarketCustSales 
		,CONVERT(DECIMAL(20,10), AVG(newCarCountHigh + myCarCountHigh + oldCarCountHigh + aCarCountHigh)) AS PlaybookInMarketCustService
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(SUM(MPVIPerformed),SUM(CPWARRRO)),3)*100) AS MPVIPenetration
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(SUM(SDSVSalesAttributed),SUM(SDSVUniqueHouseholds)),3)*100) AS SDSVNewVehicleSalesCloseRate
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(SUM(brandLoyalDealerLoyalRolling3Month),SUM(returnToMarketRolling3Month)),3)*100) AS DealerSalesLoyalty
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(SUM(emAttributedSales),SUM(retailSales)),3)*100) AS EquityMiningShareSales
		,CONVERT(DECIMAL(20,10), ROUND(GM_Special_Projects.dbo.Divide_Decimal(SUM(TotalVehiclesSold), SUM(CustomersContacted)),3)*100)  AS PlaybookBuyRate
		,CONVERT(DECIMAL(20,10), ROUND(AVG(CONVERT(DECIMAL(20,13), SDSVSalesAttributed)),0)) AS VehicleSalesFromROS
	FROM #DealerTableFull deal
	group by GROUPING sets (
		(region, slsZone, slsDistrict),
		(region, slsZone),
		(region),
		()
	)	
)p
unpivot (value for metric IN
	(EquityMiningParticipation
	,PlaybookInMarketCustSales
	,PlaybookInMarketCustService
	,MPVIPenetration 
	,SDSVNewVehicleSalesCloseRate
	,DealerSalesLoyalty
	,EquityMiningShareSales
	,PlaybookBuyRate
	,VehicleSalesFromROS)
)unpvt




/**************************
Populate DART tables  
***************************/

TRUNCATE TABLE dbo.BDCMetricDealerInfo
TRUNCATE TABLE dbo.BDCMetricMetrics
TRUNCATE TABLE dbo.BDCMetricResources
TRUNCATE TABLE dbo.BDCMetricFootnote

/**Populate Dealer Header Info**/
INSERT INTO dbo.BDCMetricDealerInfo(bac, dealerName, region, salesZone, salesDistrict, serviceDistrict, salesDistrictMgr, serviceDistrictMgr
	, divisions, equityMiningTool, thirdPartyLeads, shopClickDrive, playbook, ossTools, ebe, salesCloseRateRnk, serviceCloseRateRnk, DigitalDistrictManager, ProcareAdvocate)
SELECT deal.bac, dealerName, region, slsZone, slsDistrict, svcDistrict, slsManager, svcManager
	, divisions, ISNULL(Vendor, 'None'), NULL, NULL, CASE WHEN Flag = '1' THEN 'Y' ELSE 'N' END, NULL, enrolledEBE ,NULL , NULL, ISNULL(DDMName, 'None'), ISNULL(proCareAdvocate, 'None')
FROM #DealerTableFull deal



/**Populate metrics for DISTRICT avg. comparisons**/
INSERT INTO dbo.BDCMetricMetrics(bac, metricNameID, marketCompID, value, valueRGB, marketCompAvg, timePeriod)
select 
	deal.bac
	,deal.metric
	,field.fieldCompAvgID
	,CASE 
		WHEN deal.metric IN(8/*Equity Mining Participation*/) AND deal.value = 1 THEN 'Y' 
		WHEN deal.metric IN(8/*Equity Mining Participation*/) AND deal.value <> 1 OR deal.value IS NULL THEN 'N' 
		WHEN deal.metric IN(
				9/*Playbook-In-Market Customers Sales*/
				,14/*Playbook-In-Market Customers Service*/
				,34/*Vehicle Sales from ROs*/) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, deal.value) AS MONEY), 1), '.00', ''),'-')
		WHEN deal.metric IN(
				15/*MPVI Penetration*/
				,25/*SDSV New Vehicle Sales Close Rate*/
				,26/*Dealer Sales Loyalty*/
				,27/*Equity Mining Share of Sales*/
				,28/*Playbook-Buy Rate*/)THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), deal.value))+'%'), '-')
		ELSE CONVERT(VARCHAR(20), deal.value)
		END AS DealerValue

	,CASE 		
		WHEN deal.metric IN(8) AND deal.value > 0 THEN '226,240,217'
		WHEN deal.metric IN(8) AND deal.value = 0 THEN '244,186,186'
		WHEN deal.metric IN(25, 26, 28) AND deal.value < field.value THEN '244,186,186' 
		WHEN deal.metric IN(25, 26, 28) AND deal.value >= field.value THEN '226,240,217'
		ELSE '255,255,255' END AS MetricRGB
		
	,CASE 
		WHEN field.metric IN(9, 14, 34) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, field.value) AS MONEY), 1), '.00', ''),'-')
		WHEN field.metric IN(8, 15, 25, 26, 27, 28) THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), field.value))+'%'),'-')
		ELSE CONVERT(VARCHAR(20), field.value)
		END AS FieldValue
	,@reportPeriod
FROM #DealerData deal
LEFT JOIN #FieldData field ON deal.region = field.region
	AND deal.slsZone = field.slsZone 
	AND deal.slsDistrict = field.slsDistrict 
	AND deal.metric = field.metric


/**Populate metrics for ZONE avg. comparisons**/
INSERT INTO dbo.BDCMetricMetrics(bac, metricNameID, marketCompID, value, valueRGB, marketCompAvg, timePeriod)
select 
	deal.bac
	,deal.metric
	,field.fieldCompAvgID
	,CASE 
		WHEN deal.metric IN(8/*Equity Mining Participation*/) AND deal.value = 1 THEN 'Y' 
		WHEN deal.metric IN(8/*Equity Mining Participation*/) AND deal.value <> 1 OR deal.value IS NULL THEN 'N' 
		WHEN deal.metric IN(
				9/*Playbook-In-Market Customers Sales*/
				,14/*Playbook-In-Market Customers Service*/
				,34/*Vehicle Sales from ROs*/) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, deal.value) AS MONEY), 1), '.00', ''),'-')
		WHEN deal.metric IN(
				15/*MPVI Penetration*/
				,25/*SDSV New Vehicle Sales Close Rate*/
				,26/*Dealer Sales Loyalty*/
				,27/*Equity Mining Share of Sales*/
				,28/*Playbook-Buy Rate*/)THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), deal.value))+'%'), '-')
		ELSE CONVERT(VARCHAR(20), deal.value)
		END AS DealerValue

	,CASE 		
		WHEN deal.metric IN(8) AND deal.value > 0 THEN '226,240,217'
		WHEN deal.metric IN(8) AND deal.value = 0 THEN '244,186,186'
		WHEN deal.metric IN(25, 26, 28) AND deal.value < field.value THEN '244,186,186' 
		WHEN deal.metric IN(25, 26, 28) AND deal.value >= field.value THEN '226,240,217'
		ELSE '255,255,255' END AS MetricRGB
		
	,CASE 
		WHEN field.metric IN(9, 14, 34) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, field.value) AS MONEY), 1), '.00', ''),'-')
		WHEN field.metric IN(8, 15, 25, 26, 27, 28) THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), field.value))+'%'),'-')
		ELSE CONVERT(VARCHAR(20), field.value)
		END AS FieldValue
	,@reportPeriod
FROM #DealerData deal
LEFT JOIN #FieldData field ON deal.region = field.region 
	AND deal.slsZone = field.slsZone 
	AND deal.metric = field.metric
WHERE field.slsDistrict IS NULL


/**Populate metrics for REGION avg. comparisons**/
INSERT INTO dbo.BDCMetricMetrics(bac, metricNameID, marketCompID, value, valueRGB, marketCompAvg, timePeriod)
select 
	deal.bac
	,deal.metric
	,field.fieldCompAvgID
	,CASE 
		WHEN deal.metric IN(8/*Equity Mining Participation*/) AND deal.value = 1 THEN 'Y' 
		WHEN deal.metric IN(8/*Equity Mining Participation*/) AND deal.value <> 1 OR deal.value IS NULL THEN 'N' 
		WHEN deal.metric IN(
				9/*Playbook-In-Market Customers Sales*/
				,14/*Playbook-In-Market Customers Service*/
				,34/*Vehicle Sales from ROs*/) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, deal.value) AS MONEY), 1), '.00', ''),'-')
		WHEN deal.metric IN(
				15/*MPVI Penetration*/
				,25/*SDSV New Vehicle Sales Close Rate*/
				,26/*Dealer Sales Loyalty*/
				,27/*Equity Mining Share of Sales*/
				,28/*Playbook-Buy Rate*/)THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), deal.value))+'%'), '-')
		ELSE CONVERT(VARCHAR(20), deal.value)
		END AS DealerValue

	,CASE 		
		WHEN deal.metric IN(8) AND deal.value > 0 THEN '226,240,217'
		WHEN deal.metric IN(8) AND deal.value = 0 THEN '244,186,186'
		WHEN deal.metric IN(25, 26, 28) AND deal.value < field.value THEN '244,186,186' 
		WHEN deal.metric IN(25, 26, 28) AND deal.value >= field.value THEN '226,240,217'
		ELSE '255,255,255' END AS MetricRGB
		
	,CASE 
		WHEN field.metric IN(9, 14, 34) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, field.value) AS MONEY), 1), '.00', ''),'-')
		WHEN field.metric IN(8, 15, 25, 26, 27, 28) THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), field.value))+'%'),'-')
		ELSE CONVERT(VARCHAR(20), field.value)
		END AS FieldValue
	,@reportPeriod
FROM #DealerData deal
LEFT JOIN #FieldData field ON deal.region = field.region  
	AND deal.metric = field.metric
WHERE field.slsDistrict IS NULL
AND field.slsZone IS NULL

/**Populate metrics for NATIONAL avg. comparisons**/
INSERT INTO dbo.BDCMetricMetrics(bac, metricNameID, marketCompID, value, valueRGB, marketCompAvg, timePeriod)
select 
	deal.bac
	,deal.metric
	,field.fieldCompAvgID
	,CASE 
		WHEN deal.metric IN(8/*Equity Mining Participation*/) AND deal.value = 1 THEN 'Y' 
		WHEN deal.metric IN(8/*Equity Mining Participation*/) AND deal.value <> 1 OR deal.value IS NULL THEN 'N' 
		WHEN deal.metric IN(
				9/*Playbook-In-Market Customers Sales*/
				,14/*Playbook-In-Market Customers Service*/
				,34/*Vehicle Sales from ROs*/) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, deal.value) AS MONEY), 1), '.00', ''),'-')
		WHEN deal.metric IN(
				15/*MPVI Penetration*/
				,25/*SDSV New Vehicle Sales Close Rate*/
				,26/*Dealer Sales Loyalty*/
				,27/*Equity Mining Share of Sales*/
				,28/*Playbook-Buy Rate*/)THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), deal.value))+'%'), '-')
		ELSE CONVERT(VARCHAR(20), deal.value)
		END AS DealerValue

	,CASE 		
		WHEN deal.metric IN(8) AND deal.value > 0 THEN '226,240,217'
		WHEN deal.metric IN(8) AND deal.value = 0 THEN '244,186,186'
		WHEN deal.metric IN(25, 26, 28) AND deal.value < field.value THEN '244,186,186' 
		WHEN deal.metric IN(25, 26, 28) AND deal.value >= field.value THEN '226,240,217'
		ELSE '255,255,255' END AS MetricRGB
		
	,CASE 
		WHEN field.metric IN(9, 14, 34) THEN ISNULL(REPLACE(CONVERT(VARCHAR(20), CAST(CONVERT(INT, field.value) AS MONEY), 1), '.00', ''),'-')
		WHEN field.metric IN(8, 15, 25, 26, 27, 28) THEN ISNULL((CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,1), field.value))+'%'),'-')
		ELSE CONVERT(VARCHAR(20), field.value)
		END AS FieldValue
	,@reportPeriod
FROM #DealerData deal
LEFT JOIN #FieldData field ON deal.metric = field.metric
WHERE field.region = 'NATIONAL'


--Populate Footnotes
--truncate table dbo.BDCMetricFootnote
--select * from #DataSources

INSERT INTO dbo.BDCMetricFootnote (footnoteType, footnoteName, footnoteText, footnoteDate)
select 
	'Footer'
	, metric
	, metric + ' Data Source: ' + dataSource + '  '
	, DATENAME(MONTH, reportPeriod) + ' ' + convert(varchar(4),YEAR(reportPeriod))
from #DataSources


--Populate Resources
--TRUNCATE TABLE dbo.BDCMetricResources
/*
SELECT * FROM dbo.BDCMetricResourceLinks
SELECT * FROM dbo.BDCMetricMetricName
*/

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

--SELECT * FROM #ResourceMetricMap



if object_id('tempdb..#ResourceMetricCombined') is not null drop table #ResourceMetricCombined
select m.metricNameID, m.metricName, r.resourceLinkID, r.resourceHTML
into #ResourceMetricCombined
from dbo.BDCMetricMetricName m
join #ResourceMetricMap mr
on m.metricNameID = mr.metricNameID
join dbo.BDCMetricResourceLinks r
on mr.resourceLinkID = r.resourceLinkID

--SELECT * FROM #ResourceMetricCombined


/**Populate resources for DISTRICT avg. comparisons**/
INSERT INTO dbo.BDCMetricResources (resourceLinkID, marketCompID, bac)
SELECT CASE WHEN r.resourceLinkID in (10) and deal.value < field.value THEN r.resourceLinkID
			WHEN r.resourceLinkID in (3) and deal.value <> 1 THEN r.resourceLinkID 
		END AS resourceLinkID
	, field.fieldCompAvgID
	, deal.bac
FROM #ResourceMetricCombined r
JOIN #DealerData deal ON r.metricNameID = deal.metric
LEFT JOIN #FieldData field ON deal.region = field.region 
	AND deal.slsZone = field.slsZone 
	AND deal.slsDistrict = field.slsDistrict
	AND deal.metric = field.metric
WHERE (
(r.resourceLinkID in (10) and deal.value < field.value) OR 
(r.resourceLinkID in (3) and deal.value <> 1))


/**Populate resources for ZONE avg. comparisons**/
INSERT into dbo.BDCMetricResources (resourceLinkID, marketCompID, bac)
SELECT CASE WHEN r.resourceLinkID in (10) and deal.value < field.value THEN r.resourceLinkID
			WHEN r.resourceLinkID in (3) and deal.value <> 1 THEN r.resourceLinkID 
		END AS resourceLinkID
	, field.fieldCompAvgID
	, deal.bac
FROM #ResourceMetricCombined r
JOIN #DealerData deal ON r.metricNameID = deal.metric
LEFT JOIN #FieldData field ON deal.region = field.region 
	AND deal.slsZone = field.slsZone 
	AND deal.metric = field.metric
WHERE field.slsDistrict IS NULL
AND (
(r.resourceLinkID in (10) and deal.value < field.value) OR 
(r.resourceLinkID in (3) and deal.value <> 1))


/**Populate resources for REGION avg. comparisons**/
INSERT into dbo.BDCMetricResources (resourceLinkID, marketCompID, bac)
SELECT CASE WHEN r.resourceLinkID in (10) and deal.value < field.value THEN r.resourceLinkID
			WHEN r.resourceLinkID in (3) and deal.value <> 1 THEN r.resourceLinkID 
		END AS resourceLinkID
	, field.fieldCompAvgID
	, deal.bac
FROM #ResourceMetricCombined r
JOIN #DealerData deal ON r.metricNameID = deal.metric
LEFT JOIN #FieldData field ON deal.region = field.region  
	AND deal.metric = field.metric
WHERE field.slsDistrict IS NULL
AND field.slsZone IS NULL
AND ((r.resourceLinkID in (10) and deal.value < field.value) OR 
(r.resourceLinkID in (3) and deal.value <> 1))



/**Populate resources for NATIONAL avg. comparisons**/
INSERT into dbo.BDCMetricResources (resourceLinkID, marketCompID, bac)
SELECT CASE WHEN r.resourceLinkID in (10) and deal.value < field.value THEN r.resourceLinkID
			WHEN r.resourceLinkID in (3) and deal.value <> 1 THEN r.resourceLinkID 
		END AS resourceLinkID
	, field.fieldCompAvgID
	, deal.bac
FROM #ResourceMetricCombined r
JOIN #DealerData deal ON r.metricNameID = deal.metric
LEFT JOIN #FieldData field ON deal.metric = field.metric
WHERE field.region = 'NATIONAL'
AND ((r.resourceLinkID in (10) and deal.value < field.value) OR 
(r.resourceLinkID in (3) and deal.value <> 1))


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

--Equity Mining
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 8) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Equity Mining'))
WHERE metricNameID = 8

--Playbook Sales
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 9) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Playbook'))
WHERE metricNameID = 9

--Playbook Service
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 14) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Playbook'))
WHERE metricNameID = 14

--MPVI
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 15) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'MPVI'))
WHERE metricNameID = 15

--SDSV
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 25) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'SDSV'))
WHERE metricNameID = 25

--DSL
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 26) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Dealer Sales Loyalty'))
WHERE metricNameID = 26

--Equity Mining Share of Sales
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 27) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Equity Mining'))
WHERE metricNameID = 27

--Playbook-Buy Rate
UPDATE dbo.BDCMetricMetricName
SET metricTooltip =
	((SELECT metricTooltip FROM #MetricToolTip WHERE metricNameID = 28) + ' ' + (SELECT footnoteDate FROM dbo.BDCMetricFootnote WHERE footnoteName = 'Playbook'))
WHERE metricNameID = 28

				
				

EXEC dbo.BDCMetricPopulateSalesDPRData

EXEC dbo.BDCMetricPopulateServiceDPRData

EXEC dbo.BDCMetricPopulateSSOData

EXEC dbo.BDCMetricPopulateDefaultResources


