using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;
using MessagePack.Internal;
using Microsoft.Data.SqlClient.Server;
using Microsoft.Extensions.Primitives;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace CluedIn.Connector.SqlServer.Unit.Tests
{
    public class TemporaryUtilityTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public TemporaryUtilityTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        private ConnectorEntityData GetBaseConnectorEntity(Guid entityId, EntityCode originEntityCode, EntityEdge[] outgoingEdges)
        {
            return new ConnectorEntityData(
                VersionChangeType.Added,
                StreamMode.Sync,
                entityId,
                persistInfo: null,
                previousPersistInfo: null,
                originEntityCode,
                EntityType.Person,
                properties: Array.Empty<ConnectorPropertyData>(),
                entityCodes: Array.Empty<IEntityCode>(),
                incomingEdges: Array.Empty<EntityEdge>(),
                outgoingEdges);
        }

        private SqlConnectorEntityData GetSqlConnectorEntityData(Guid entityId, EntityCode originEntityCode, EntityEdge[] outgoingEdges, Guid correlationId, DateTimeOffset timestamp)
        {
            var baseConnectorEntityData = GetBaseConnectorEntity(entityId, originEntityCode, outgoingEdges);
            return new SqlConnectorEntityData(baseConnectorEntityData, correlationId, timestamp);
        }

        private StreamModel GetStreamModel()
        {
            return new StreamModel() { ContainerName = "SyncTable", Mode = StreamMode.Sync };
        }

        private string GetCommandTextWithParameters(SqlServerConnectorCommand command)
        {
            var builder = new StringBuilder();

            foreach (var commandParameter in command.Parameters)
            {
                if (commandParameter.Value is SqlDataRecord[] dataRecords)
                {
                    builder.AppendLine($"Declare {commandParameter.ParameterName} {commandParameter.TypeName};");

                    foreach (var dataRecord in dataRecords)
                    {
                        builder.Append($"INSERT INTO {commandParameter.ParameterName} VALUES(");
                        var i = 0;
                        var values = new List<object>();
                        while (true)
                        {
                            try
                            {
                                var value = dataRecord[i++];
                                values.Add(value);
                            }
                            catch (IndexOutOfRangeException)
                            {
                                break;
                            }
                        }

                        builder.Append(string.Join(", ", values.Select(value => $"'{value}'")));
                        builder.Append(")");
                        builder.Append(Environment.NewLine);
                    }
                }
                else
                {
                    var typeName = string.IsNullOrEmpty(commandParameter.TypeName)
                        ? commandParameter.SqlDbType.ToString()
                        : commandParameter.TypeName;
                    var valueAssignment = commandParameter.Value != null
                        ? $" = '{commandParameter.Value}'"
                        : "";
                    builder.AppendLine($"DECLARE {commandParameter.ParameterName} { typeName } {valueAssignment};");
                }
            }

            builder.AppendLine();
            builder.AppendLine(command.Text);

            return builder.ToString();
        }

        [Theory, AutoNData]
        public void TemporaryTest(EntityCode originEntityCode, Guid correlationId, DateTimeOffset timestamp)
        {
            var entityId = Guid.NewGuid();
            var schema = SqlName.FromSanitized("dbo");
            var streamModel = GetStreamModel();
            var edgeType = EntityEdgeType.For;

            var fromReference = new EntityReference(originEntityCode);
            var entityReference1 = new EntityReference(new EntityCode(EntityType.Organization, CodeOrigin.CluedIn, 1));
            var entityReference2 = new EntityReference(new EntityCode(EntityType.Organization, CodeOrigin.CluedIn, 2));

            // No edges and no edge property
            {
                var edges = Array.Empty<EntityEdge>();

                var baseSqlConnectorEntityData = GetSqlConnectorEntityData(entityId, originEntityCode, edges, correlationId, timestamp);
                var edgeInsertCommand = GetCommandTextWithParameters(EdgeTableDefinition.CreateUpsertCommand(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));
                var edgePropertiesInsertCommand = GetCommandTextWithParameters(EdgePropertiesTableDefinition.CreateUpsertCommands(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));

                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("-- No edge, no property");
                _testOutputHelper.WriteLine(edgeInsertCommand);
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine(edgePropertiesInsertCommand);
                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine("----------------------------------------------------------------------");
            }
            
            // Edge and no edge property
            {
                var edge = new EntityEdge(fromReference, entityReference1, edgeType);
                var edges = new EntityEdge[] { edge };
                
                var baseSqlConnectorEntityData = GetSqlConnectorEntityData(entityId, originEntityCode, edges, correlationId, timestamp);
                var edgeInsertCommand = GetCommandTextWithParameters(EdgeTableDefinition.CreateUpsertCommand(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));
                var edgePropertiesInsertCommand = GetCommandTextWithParameters(EdgePropertiesTableDefinition.CreateUpsertCommands(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));

                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("-- Edge, no property");
                _testOutputHelper.WriteLine(edgeInsertCommand);
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine(edgePropertiesInsertCommand);
                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine("----------------------------------------------------------------------");
            }

            // Edge and edge property key 1 value 1
            {
                var edge = new EntityEdge(fromReference, entityReference1, edgeType);
                edge.Properties.Add("PropertyKey1", "PropertyValue1");
                var edges = new EntityEdge[] { edge };

                var baseSqlConnectorEntityData = GetSqlConnectorEntityData(entityId, originEntityCode, edges, correlationId, timestamp);
                var edgeInsertCommand = GetCommandTextWithParameters(EdgeTableDefinition.CreateUpsertCommand(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));
                var edgePropertiesInsertCommand = GetCommandTextWithParameters(EdgePropertiesTableDefinition.CreateUpsertCommands(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));

                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("-- Edge and edge property key 1 value 1");
                _testOutputHelper.WriteLine(edgeInsertCommand);
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine(edgePropertiesInsertCommand);
                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine("----------------------------------------------------------------------");
            }

            // Edge and edge property key 1 value 2
            {
                var edge = new EntityEdge(fromReference, entityReference1, edgeType);
                edge.Properties.Add("PropertyKey1", "PropertyValue2");
                var edges = new EntityEdge[] { edge };

                var baseSqlConnectorEntityData = GetSqlConnectorEntityData(entityId, originEntityCode, edges, correlationId, timestamp);
                var edgeInsertCommand = GetCommandTextWithParameters(EdgeTableDefinition.CreateUpsertCommand(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));
                var edgePropertiesInsertCommand = GetCommandTextWithParameters(EdgePropertiesTableDefinition.CreateUpsertCommands(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));

                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("-- Edge and edge property key 1 value 2");
                _testOutputHelper.WriteLine(edgeInsertCommand);
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine(edgePropertiesInsertCommand);
                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine("----------------------------------------------------------------------");
            }

            // Edge and edge property key 2 value 1
            {
                var edge = new EntityEdge(fromReference, entityReference1, edgeType);
                edge.Properties.Add("PropertyKey2", "PropertyValue1");
                var edges = new EntityEdge[] { edge };

                var baseSqlConnectorEntityData = GetSqlConnectorEntityData(entityId, originEntityCode, edges, correlationId, timestamp);
                var edgeInsertCommand = GetCommandTextWithParameters(EdgeTableDefinition.CreateUpsertCommand(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));
                var edgePropertiesInsertCommand = GetCommandTextWithParameters(EdgePropertiesTableDefinition.CreateUpsertCommands(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));

                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("-- Edge and edge property key 2 value 1");
                _testOutputHelper.WriteLine(edgeInsertCommand);
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine(edgePropertiesInsertCommand);
                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine("----------------------------------------------------------------------");
            }

            // Different Edge and no property
            {
                var edge = new EntityEdge(fromReference, entityReference2, edgeType);
                var edges = new EntityEdge[] { edge };

                var baseSqlConnectorEntityData = GetSqlConnectorEntityData(entityId, originEntityCode, edges, correlationId, timestamp);
                var edgeInsertCommand = GetCommandTextWithParameters(EdgeTableDefinition.CreateUpsertCommand(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));
                var edgePropertiesInsertCommand = GetCommandTextWithParameters(EdgePropertiesTableDefinition.CreateUpsertCommands(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));

                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("-- Different Edge and no property");
                _testOutputHelper.WriteLine(edgeInsertCommand);
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine(edgePropertiesInsertCommand);
                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine("----------------------------------------------------------------------");
            }

            // Different Edge and property
            {
                var edge = new EntityEdge(fromReference, entityReference2, edgeType);
                edge.Properties.Add("PropertyKey1", "PropertyValue1");
                var edges = new EntityEdge[] { edge };

                var baseSqlConnectorEntityData = GetSqlConnectorEntityData(entityId, originEntityCode, edges, correlationId, timestamp);
                var edgeInsertCommand = GetCommandTextWithParameters(EdgeTableDefinition.CreateUpsertCommand(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));
                var edgePropertiesInsertCommand = GetCommandTextWithParameters(EdgePropertiesTableDefinition.CreateUpsertCommands(streamModel, EdgeDirection.Outgoing, baseSqlConnectorEntityData, schema));

                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("-- Different Edge and property");
                _testOutputHelper.WriteLine(edgeInsertCommand);
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine(edgePropertiesInsertCommand);
                _testOutputHelper.WriteLine("");
                _testOutputHelper.WriteLine("GO");
                _testOutputHelper.WriteLine("----------------------------------------------------------------------");
            }
        }
    }
}
