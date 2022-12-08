using System;
using System.Collections.Generic;
using CluedIn.Core.Data.Vocabularies;

namespace CluedIn.Connector.SqlServer.Features
{
    public class SqlColumnHelper
    {
        private static readonly IDictionary<string, string> _knownColumnTypes = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
        {
            {"id", "uniqueidentifier"},
            {"originentitycode", "nvarchar(1024)"},
            {"codes", "nvarchar(1024)"},
            {"code", "nvarchar(1024)"},  // used in edges table
            {"correlationid", "nvarchar(1024)"}, // used in edges and codes table
        };

        public static string GetColumnType(VocabularyKeyDataType type, string columnName)
        {
            var column = columnName.ToLower();
            if (_knownColumnTypes.ContainsKey(column))
                return _knownColumnTypes[column];

            // return type switch //TODO: @LJU: Disabled as it needs reviewing; Breaks streams;
            // {
            //     VocabularyKeyDataType.Integer => "bigint",
            //     VocabularyKeyDataType.Number => "decimal(18,4)",
            //     VocabularyKeyDataType.Money => "money",
            //     VocabularyKeyDataType.DateTime => "datetime2",
            //     VocabularyKeyDataType.Time => "time",
            //     VocabularyKeyDataType.Xml => "XML",
            //     VocabularyKeyDataType.Guid => "uniqueidentifier",
            //     VocabularyKeyDataType.GeographyLocation => "geography",
            //     _ => "nvarchar(max)"
            // };

            return "nvarchar(max)";
        }
    }
}
