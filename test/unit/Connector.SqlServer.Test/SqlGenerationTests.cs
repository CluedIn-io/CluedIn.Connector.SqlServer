﻿using System;
using System.Collections.Generic;
using AutoFixture.Xunit2;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
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

        [Theory, InlineAutoData]
        public void CreateContainerWorks(string name)
        {
            var model = new CreateContainerModel
            {
                Name = name,
                DataTypes = new List<ConnectionDataType>
                {
                    new ConnectionDataType { Name = "Field1", Type = VocabularyKeyDataType.Integer },
                    new ConnectionDataType { Name = "Field2", Type = VocabularyKeyDataType.Text },
                    new ConnectionDataType { Name = "Field3", Type = VocabularyKeyDataType.DateTime },
                    new ConnectionDataType { Name = "Field4", Type = VocabularyKeyDataType.Number },
                    new ConnectionDataType { Name = "Field5", Type = VocabularyKeyDataType.Boolean },
                }
            };

            var result = Sut.BuildCreateContainerSql(model);

            Assert.Equal($"CREATE TABLE [{name}]( [Field1] bigint NULL, [Field2] nvarchar(max) NULL, [Field3] datetime2 NULL, [Field4] decimal(18,4) NULL, [Field5] nvarchar(max) NULL ) ON[PRIMARY]", result.Trim().Replace(Environment.NewLine, " "));
        }

        [Theory, InlineAutoData]
        public void StoreDataWorks(string name, int field1, string field2, DateTime field3, decimal field4, bool field5)
        {
            var data = new Dictionary<string, object>
                        {
                             { "Field1", field1   },
                             { "Field2", field2   },
                             { "Field3", field3  },
                             { "Field4", field4   },
                             { "Field5", field5   }
                        };

            var result = Sut.BuildStoreDataSql(name, data, out var param);

            Assert.Equal($"INSERT INTO [{name}] ([Field1],[Field2],[Field3],[Field4],[Field5]) VALUES (@Field1,@Field2,@Field3,@Field4,@Field5)", result.Trim());
            Assert.Equal(data.Count, param.Count);

            for (var index = 0; index < data.Count; index++)
            {
                var parameter = param[index];
                var val = data[$"Field{index + 1}"];
                Assert.Equal(val, parameter.Value);
            }
        }
    }
}