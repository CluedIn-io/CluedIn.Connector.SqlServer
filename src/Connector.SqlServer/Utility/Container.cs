using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;
using System.Collections.Generic;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Utility
{
    public class Container
    {
        private static readonly ConnectionDataType[] _codeEdgeColumns = {
            new ConnectionDataType { Name = "OriginEntityCode", Type = VocabularyKeyDataType.Text },
            new ConnectionDataType { Name =  "Code", Type = VocabularyKeyDataType.Text }
        };

        private static readonly ConnectionDataType[] _eventStreamCodeEdgeColumns = { new ConnectionDataType { Name = "CorrelationId", Type = VocabularyKeyDataType.Text }, };        

        public Container(SanitizedSqlString containerName) : this(containerName, StreamMode.Sync)
        {
        }

        public Container(string containerName, StreamMode mode) : this(new SanitizedSqlString(containerName), mode)
        {
        }        

        public Container(SanitizedSqlString containerName, StreamMode mode)
        {
            ContainerName = containerName;
            PrimaryTable = containerName;

            var columns = _codeEdgeColumns;

            if (mode == StreamMode.EventStream)
                columns = columns.Union(_eventStreamCodeEdgeColumns).ToArray();

            Tables = new Dictionary<string, Table>
            {
                ["Codes"] = new Table(new SanitizedSqlString($"{ContainerName}Codes"), columns, _codeEdgeColumns.Select(x => x.Name)),
                ["Edges"] = new Table(new SanitizedSqlString($"{ContainerName}Edges"), columns, _codeEdgeColumns.Select(x => x.Name)),
            };
        }

        public SanitizedSqlString PrimaryTable { get; }
        public SanitizedSqlString ContainerName { get; }

        public IReadOnlyDictionary<string, Table> Tables { get; }

        public class Table
        {
            public Table(
                SanitizedSqlString name,
                IEnumerable<ConnectionDataType> columns,
                IEnumerable<string> keys)
            {
                Name = name;
                Columns = new List<ConnectionDataType>(columns).AsReadOnly();
                Keys = new List<string>(keys).AsReadOnly();
            }

            public SanitizedSqlString Name { get; }

            public IReadOnlyList<ConnectionDataType> Columns { get; }

            public IReadOnlyList<string> Keys { get; }
        }
    }
}
