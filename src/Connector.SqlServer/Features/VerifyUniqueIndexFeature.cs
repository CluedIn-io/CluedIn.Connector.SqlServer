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

        private string BuildVerifyIndexCommand(string indexName, SqlTableName tableName, string createIndexCommand)
        {
            return
 $@"IF EXISTS 
	(SELECT * 
	FROM sys.indexes 
	WHERE name = '{indexName}' AND is_unique = 'false') 
BEGIN
	DROP INDEX [{indexName}] ON {tableName.FullyQualifiedName}

	{createIndexCommand}
END";
        }
    }
}
