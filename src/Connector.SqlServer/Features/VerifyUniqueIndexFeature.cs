using CluedIn.Connector.SqlServer.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Features
{
    internal sealed class VerifyUniqueIndexFeature
    {
        public string GetVerifyUniqueIndexCommand(IBuildCreateIndexFeature createIndexFeature, SqlTableName tableName, IEnumerable<(string[] columns, bool isUnique)> indexKeys)
        {
            var indexName = createIndexFeature.GetIndexName(tableName);
            var createIndexCommands = string.Join(
                Environment.NewLine,
                indexKeys.Select(key => createIndexFeature.GetCreateIndexCommandText(tableName, key.columns, key.isUnique)));
            var verifyUniqueIndexCommand = BuildVerifyIndexCommand(indexName, tableName, createIndexCommands);

            return verifyUniqueIndexCommand;
        }

        private static string BuildVerifyIndexCommand(string indexName, SqlTableName tableName, string createIndexCommand)
        {
            return $@"
Declare @EntitiesWithDuplicatesExists AS BIT
SET @EntitiesWithDuplicatesExists = 0

IF EXISTS (
  SELECT TOP(1) [Id]
  FROM {tableName.FullyQualifiedName}
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
  IF EXISTS (
    SELECT *
    FROM sys.indexes
    WHERE
      object_id = (SELECT object_id FROM sys.objects WHERE name = '{tableName.LocalName}') AND
      name = '{indexName}' AND
      is_unique = 'false')
  BEGIN
    PRINT 'Adding index'
	DROP INDEX {indexName} ON {tableName.FullyQualifiedName}
	{createIndexCommand}
  END

  SELECT 1
END";
        }
    }
}
