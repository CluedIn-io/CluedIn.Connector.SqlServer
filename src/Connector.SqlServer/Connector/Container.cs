using System.Collections.Generic;
using System.Linq;
using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class Container
    {
        private static readonly ConnectionDataType[] _codeEdgeColumns = {
            new ConnectionDataType { Name = "OriginEntityCode", Type = VocabularyKeyDataType.Text },
            new ConnectionDataType { Name =  "Code", Type = VocabularyKeyDataType.Text }
        };

        private static readonly ConnectionDataType[] _eventStreamCodeEdgeColumns = {new ConnectionDataType {Name = "CorrelationId", Type = VocabularyKeyDataType.Text},};

        public Container(string containerName) : this(containerName, StreamMode.Sync)
        {
            
        }
        public Container(string containerName, StreamMode mode)
        {
            PrimaryTable = containerName.ToSanitizedSqlName();
            var columns = _codeEdgeColumns;

            if (mode == StreamMode.EventStream)
            {
                columns = columns.Union(_eventStreamCodeEdgeColumns).ToArray();
            }

            Tables = new Dictionary<string, Table>
            {
                ["Codes"] = new Table($"{PrimaryTable}Codes", columns, _codeEdgeColumns.Select(x => x.Name)),
                ["Edges"] = new Table($"{PrimaryTable}Edges", columns, _codeEdgeColumns.Select(x => x.Name)),
            };
        }

        public string PrimaryTable { get; }

        public IReadOnlyDictionary<string, Table> Tables { get; }

        public class Table
        {
            public Table(
                string name,
                IEnumerable<ConnectionDataType> columns,
                IEnumerable<string> keys)
            {
                Name = name.ToSanitizedSqlName();
                Columns = new List<ConnectionDataType>(columns).AsReadOnly();
                Keys = new List<string>(keys).AsReadOnly();
            }

            public string Name { get; }

            public IReadOnlyList<ConnectionDataType> Columns { get; }

            public IReadOnlyList<string> Keys { get; }
        }
    }
}
