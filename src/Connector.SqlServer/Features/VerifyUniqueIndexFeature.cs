using CluedIn.Connector.SqlServer.Utils;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer.Features
{
    internal sealed class VerifyUniqueIndexFeature
    {
        public string GetVerifyUniqueIndexCommand(IBuildCreateIndexFeature createIndexFeature, SqlTableName tableName, IEnumerable<string> indexKeys)
        {
            var indexName = createIndexFeature.GetIndexName(tableName);
            var createIndexCommand = createIndexFeature.GetCreateIndexCommandText(tableName, indexKeys, true);
            var verifyUniqueIndexCommand = BuildVerifyIndexCommand(indexName, tableName, createIndexCommand);

            return verifyUniqueIndexCommand;
        }

        private static string BuildVerifyIndexCommand(string indexName, SqlTableName tableName, string createIndexCommand)
        {
            return $@"
Declare @EntitiesWithDuplicatesExists AS BIT
SET @EntitiesWithDuplicatesExists = 0

IF EXISTS (
SELECT TOP(1) [Id]
   FROM {tableName}
   GROUP BY [Id]
   HAVING COUNT(Id) > 1)
BEGIN
  SET @EntitiesWithDuplicatesExists = 1
END

If @EntitiesWithDuplicatesExists = 1
BEGIN
  PRINT 'Cannot add unique index, since duplicates exist'
  SELECT 0
END
ELSE
BEGIN
  IF EXISTS (SELECT * FROM sys.indexes WHERE name = '{indexName}' AND is_unique = 'false')
  BEGIN
    PRINT 'Adding index'
	DROP INDEX {indexName} ON {tableName}
	{createIndexCommand}
  END

  SELECT 1
END";
        }
    }
}
