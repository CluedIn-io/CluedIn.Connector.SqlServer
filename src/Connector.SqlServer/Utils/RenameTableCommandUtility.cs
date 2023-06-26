using CluedIn.Connector.SqlServer.Connector;
using Microsoft.Data.SqlClient;
using System;
using System.Data;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class RenameTableCommandUtility
    {
        public static SqlServerConnectorCommand BuildTableRenameCommand(SqlTableName oldTableName, SqlTableName newTableName, SqlName schema, DateTimeOffset suffixDate)
        {
            var text = @$"
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @OldTableName AND TABLE_SCHEMA = @Schema)
BEGIN
	DECLARE @FullOldTableName SYSNAME = @Schema + N'.' + @OldTableName
    EXEC sp_rename @FullOldTableName, @NewTableName;
END

WHILE EXISTS(
	SELECT [CONSTRAINT_NAME] 
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	WHERE 
		[TABLE_NAME] = @NewTableName 
		AND 
		NOT [CONSTRAINT_NAME] LIKE '%' + @ArchiveSuffix)
BEGIN
	DECLARE @ConstraintName SYSNAME;
	SELECT TOP 1 @ConstraintName = [CONSTRAINT_NAME] 
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	WHERE 
		[TABLE_NAME] = @NewTableName
		AND 
		NOT [CONSTRAINT_NAME] LIKE '%' + @ArchiveSuffix;

	DECLARE @FullConstraintName SYSNAME = @Schema + '.' + @ConstraintName;
	DECLARE @NewConstraintName SYSNAME = @ConstraintName + @archiveSuffix;
	EXEC sp_rename @objname = @FullConstraintName, @newname = @NewConstraintName, @objtype = N'OBJECT';
END";

            var schemaParameter = new SqlParameter("@Schema", SqlDbType.NVarChar) { Value = schema.ToString() };
            var oldTableNameParameter = new SqlParameter("@OldTableName", SqlDbType.NVarChar) { Value = oldTableName.LocalName.ToString() };
            var newTableNameParameter = new SqlParameter("@NewTableName", SqlDbType.NVarChar) { Value = newTableName.LocalName.ToString() };
            var archiveSuffixParameter = new SqlParameter("@ArchiveSuffix", SqlDbType.NVarChar) { Value = suffixDate.ToString("yyyyMMddHHmmss") };
            var parameters = new[] { schemaParameter, oldTableNameParameter, newTableNameParameter, archiveSuffixParameter };

            return new SqlServerConnectorCommand { Text = text, Parameters = parameters};
        }
    }
}
