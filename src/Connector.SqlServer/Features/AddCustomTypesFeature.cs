namespace CluedIn.Connector.SqlServer.Features
{
    public class AddCustomTypesFeature : IAddCustomTypesFeature
    {
        public string GetCreateCustomTypesCommandText()
        {
            return @"
IF Type_ID(N'CodeTableType') IS NULL
BEGIN
  CREATE TYPE CodeTableType AS TABLE( Code nvarchar(1024));
END

GRANT EXEC ON CodeTableType TO PUBLIC
";
        }
    }
}
