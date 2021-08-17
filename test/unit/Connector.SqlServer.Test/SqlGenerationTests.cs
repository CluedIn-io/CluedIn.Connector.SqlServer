using System;
using System.Collections.Generic;
using System.Linq;
using AutoFixture.Xunit2;
using CluedIn.Core.Streams.Models;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests
{
    public class SqlGenerationTests : SqlServerConnectorTestsBase
    {
        [Theory, InlineAutoData]
        public void EmptyContainerWorks(string name)
        {
            var result = Sut.BuildEmptyContainerSql(name);

            Assert.Equal($"TRUNCATE TABLE [{name}]", result.Trim());
        }

        // [Theory, InlineAutoData]
        // public void CreateContainerWorks(string name)
        // {
        //     var model = new CreateContainerModel
        //     {
        //         Name = name,
        //         DataTypes = new List<ConnectionDataType>
        //         {
        //             new ConnectionDataType { Name = "Field1", Type = VocabularyKeyDataType.Integer },
        //             new ConnectionDataType { Name = "Field2", Type = VocabularyKeyDataType.Text },
        //             new ConnectionDataType { Name = "Field3", Type = VocabularyKeyDataType.DateTime },
        //             new ConnectionDataType { Name = "Field4", Type = VocabularyKeyDataType.Number },
        //             new ConnectionDataType { Name = "Field5", Type = VocabularyKeyDataType.Boolean },
        //         }
        //     };
        //
        //     var result = Sut.BuildCreateContainerSql(model.Name, model.DataTypes);
        //
        //     Assert.Equal($"CREATE TABLE [{name}]( [Field1] bigint NULL, [Field2] nvarchar(max) NULL, [Field3] datetime2 NULL, [Field4] decimal(18,4) NULL, [Field5] nvarchar(max) NULL) ON[PRIMARY]", result.Trim().Replace(Environment.NewLine, " "));
        // }

        [Theory, InlineAutoData]
        public void StoreEdgeDataWorks(string name, string originEntityCode, string correlationId, List<string> edges)
        {
            var result = Sut.BuildEdgeStoreDataSql(name, originEntityCode, correlationId, edges, out var param);
            Assert.Equal(edges.Count + 2, param.Count); // params will also include origin entity code
            Assert.Contains(param, p => p.ParameterName == "@OriginEntityCode" && p.Value.Equals(originEntityCode));
            for (var index = 0; index < edges.Count; index++)
            {
                Assert.Contains(param, p => p.ParameterName == $"@{index}" && p.Value.Equals(edges[index]));
            }

            var expectedLines = new List<string>
            {
                $"DELETE FROM [{name}] where [OriginEntityCode] = @OriginEntityCode",
                $"INSERT INTO [{name}] ([OriginEntityCode],[Code]) values",
                string.Join(", ", Enumerable.Range(0, edges.Count).Select(i => $"(@OriginEntityCode, @{i})"))
            };

            var expectedSql = string.Join(Environment.NewLine, expectedLines);
            Assert.Equal(expectedSql, result.Trim());
        }

        [Theory, InlineAutoData]
        public void StoreEdgeData_NoEdges_Works(string name, string originEntityCode, string correlationId)
        {
            var edges = new List<string>();
            var result = Sut.BuildEdgeStoreDataSql(name, originEntityCode, correlationId, edges, out var param);
            Assert.Equal(2, param.Count); // params will also include origin entity code and correlationid
            Assert.Contains(param, p => p.ParameterName == "@OriginEntityCode" && p.Value.Equals(originEntityCode));
            Assert.Equal($"DELETE FROM [{name}] where [OriginEntityCode] = @OriginEntityCode", result.Trim());
        }
    }
}
