-- ETL QUERIES:

-- data flow source query
SELECT
	a.WorkOrderID,
	a.OperationSequence,
	c.ProductNumber as ProductAlternateKey,
	a.LocationID as WorkStationAlternateKey,
	cast(a.ScheduledStartDate as date) as ScheduledStartDateAlternateKey,
	cast(a.ScheduledEndDate as date) as ScheduledEndDateAlternateKey,
	cast(a.ActualStartDate as date) as ActualStartDateAlternateKey,
	cast(a.ActualEndDate as date) as ActualEndDateAlternateKey,
	cast(b.StartDate as date) as StartDateAlternateKey,
	cast(b.EndDate as date) as EndDateAlternateKey,
	cast(b.DueDate as date) as DueDateAlternateKey,
	a.ActualResourceHrs as ResourceHours,
	a.PlannedCost,
	a.ActualCost,
	IIF(a.ModifiedDate > b.ModifiedDate, a.ModifiedDate, b.ModifiedDate) as LastModifiedDateTime
FROM Production.WorkOrderRouting a
LEFT OUTER JOIN Production.WorkOrder b
	ON a.WorkOrderID = b.WorkOrderID
LEFT OUTER JOIN Production.Product c
	ON	b.ProductID=c.ProductID
WHERE (
	a.ModifiedDate >= ? OR 
	b.ModifiedDate >= ?
	);


-- get last modified date time		
SELECT coalesce(max(LastModifiedDateTime), cast('01/01/1900' as date)) as LastModifiedDateTime
FROM FactWorkOrderFulfillment;


-- lookup queries
SELECT 
	ProductKey,
	ProductAlternateKey
FROM DimProduct
WHERE EndDate Is Null;

SELECT 
	WorkStationKey,
	WorkStationAlternateKey
FROM DimWorkstation
WHERE EndDate Is NULL;

SELECT
	DateKey,
	FullDateAlternateKey
FROM DimDate;


-- staging merge
merge	FactWorkOrderFulfillment as target
using	staging_FactWorkOrderFulfillment as source
	on	coalesce(source.WorkOrderID,-1)=coalesce(target.WorkOrderID,-1) AND
		coalesce(source.OperationSequence,-1)=coalesce(target.OperationSequence,-1)
when matched then 
	update set
		ProductKey=source.ProductKey,
		WorkstationKey=source.WorkstationKey,
		ScheduledStartDateKey=source.ScheduledStartDateKey,
		ScheduledEndDateKey=source.ScheduledEndDateKey,
		ActualStartDateKey=source.ActualStartDateKey,
		ActualEndDateKey=source.ActualEndDateKey,
		WorkOrderStartDateKey=source.WorkOrderStartDateKey,
		WorkOrderEndDateKey=source.WorkOrderEndDateKey,
		WorkOrderDueDateKey=source.WorkOrderDueDateKey,
		ResourceHours=source.ResourceHours,
		PlannedCost=source.PlannedCost,
		ActualCost=source.ActualCost,
		LastModifiedDateTime=source.LastModifiedDateTime
when not matched then 
	insert (
		WorkOrderID,
		OperationSequence,
		ProductKey,
		WorkstationKey,
		ScheduledStartDateKey,
		ScheduledEndDateKey,
		ActualStartDateKey,
		ActualEndDateKey,
		WorkOrderStartDateKey,
		WorkOrderEndDateKey,
		WorkOrderDueDateKey,
		ResourceHours,
		PlannedCost,
		ActualCost,
		LastModifiedDateTime
		)
	values(
		COALESCE(source.WorkOrderID,-1),
		COALESCE(source.OperationSequence,-1),
		source.ProductKey,
		source.WorkstationKey,
		source.ScheduledStartDateKey,
		source.ScheduledEndDateKey,
		source.ActualStartDateKey,
		source.ActualEndDateKey,
		source.WorkOrderStartDateKey,
		source.WorkOrderEndDateKey,
		source.WorkOrderDueDateKey,
		source.ResourceHours,
		source.PlannedCost,
		source.ActualCost,
		source.LastModifiedDateTime
		);


-- staging truncate 
truncate table staging_FactWorkOrderFulfillment;