We're adding a unique index on the id column, so we need to make sure that there are no duplicates in the export tables.

These steps should only be done while no streaming is happening

# (Optional) Copy duplicates to seperate table
Use the code below to Copies all duplicates to a new table.
This moves all but the latest duplicate (as specified by `TimeStamp`) to a new table.
Be sure to change `<TableName>` to the name of your primary table

```sql
SELECT Original.*
INTO [<DataBaseName>].[dbo].[<TableName>_Duplicates]
FROM [<DataBaseName>].[dbo].[<TableName>] Original
JOIN (SELECT *
  FROM (
    SELECT
	  [Id],
	  [TimeStamp],
	  ROW_NUMBER() OVER(PARTITION BY [Id] ORDER BY [TimeStamp] DESC) AS [DuplicateNumber]
	  FROM [<DataBaseName>].[dbo].[<TableName>]) WithDuplicateNumbers
	WHERE WithDuplicateNumbers.[DuplicateNumber] > 1) Duplicates
ON Original.Id = Duplicates.Id AND Original.TimeStamp = Duplicates.TimeStamp
```

# Delete duplicates
Use the code below to delete all duplicate.
This deletes all but the latest duplicate (as specified by `TimeStamp`).
Be sure to change `<TableName>` to the name of your primary table

```sql
WITH cte AS (
  SELECT 
    [Id],
	[TimeStamp],
	ROW_NUMBER() OVER(PARTITION BY [Id] ORDER BY [TimeStamp] DESC) AS [DuplicateNumber]
  FROM [<DataBaseName>].[dbo].[<TableName>]
)
DELETE FROM cte
WHERE [DuplicateNumber] > 1
```