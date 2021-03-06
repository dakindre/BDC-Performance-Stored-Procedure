--Create Dealer List
if object_id('tempdb..#DealerList') is not null drop table #DealerList
SELECT bac 
INTO #DealerList
FROM BDCMetricDealerInfo

--Create Empty Resource Matrix
if object_id('tempdb..#ResourceMatrix') is not null drop table #ResourceMatrix
SELECT *, NULL AS item
INTO #ResourceMatrix
FROM(
	SELECT *
	FROM #DealerList
	CROSS JOIN (SELECT '1'
		UNION SELECT '2'
		UNION SELECT '3'
		UNION SELECT '4')marketCompID(marketCompID))m
CROSS JOIN (SELECT '1' UNION SELECT '2')l(leadID)
			
/**
SELECT * FROM #EmptyMatrix
WHERE bac = '111117'
**/

--Populate Unique Existing Resource Table
if object_id('tempdb..#IncludeResource') is not null drop table #IncludeResource
SELECT DISTINCT r.bac, r.marketCompID, l.leadID
INTO #IncludeResource
FROM BDCMetricResources r
JOIN BDCMetricResourceLinks l ON r.resourceLinkID = l.resourceLinkID

UPDATE #ResourceMatrix 
	SET #ResourceMatrix.item = 1 --Arbitrary Value(Just Don't set to Default ResourceLinkID for Generating or Managing in the Resource File)
FROM #ResourceMatrix rm INNER JOIN #IncludeResource ir ON rm.bac = ir.bac AND rm.marketCompID = ir.marketCompID AND rm.leadID = ir.leadID

--Create Table To Import to BDCMetricResources
if object_id('tempdb..#IncludeResource') is not null drop table #IncludeResource
SELECT	bac
		,marketCompID
		,leadID
		,CASE WHEN leadID = '1' THEN '16' 
		 WHEN leadID = '2' THEN '17' END AS item
INTO #ResourcesToImport	 
FROM #ResourceMatrix WHERE item IS NULL

--Update BDCMetricResource Table to include new Default Links
INSERT into dbo.BDCMetricResources (resourceLinkID, marketCompID, bac)
	SELECT item, marketCompID, bac
	FROM #ResourcesToImport
	
	
--Populate orderID
UPDATE r
SET orderID = orderIDUpdate
FROM dbo.BDCMetricResources r
JOIN (
	SELECT *, row_number()over(partition by bac, marketCompID order by resourceLinkID) as orderIDUpdate
	FROM dbo.BDCMetricResources
) rr
ON r.resourceID = rr.resourceID 


