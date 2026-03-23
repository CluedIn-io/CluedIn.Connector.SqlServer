# Minimum Required SQL Server Permissions

This document describes the minimum SQL Server permissions required for the CluedIn SQL Server Connector to operate correctly.

## Overview

The SQL Server Connector requires specific permissions to create tables, store data, and manage export streams. When configuring a SQL Server connection, ensure the database user has all the permissions listed below.

## Quick Setup

For a quick setup, you can use the following SQL script to grant all required permissions to your connector user:

```sql
-- Replace 'YourDatabaseName' with your target database name
-- Replace 'YourSchema' with your target schema (default is 'dbo')
-- Replace 'CluedInConnector' with your SQL user name

USE [YourDatabaseName];
GO

-- Grant database-level permissions
GRANT CREATE TABLE TO [CluedInConnector];
GRANT CREATE TYPE TO [CluedInConnector];

-- Grant schema-level permissions
GRANT ALTER ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT EXECUTE ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT SELECT ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT INSERT ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT UPDATE ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT DELETE ON SCHEMA::[YourSchema] TO [CluedInConnector];
```

## Detailed Permission Requirements

### Database-Level Permissions

| Permission | Purpose |
|------------|---------|
| **CREATE TABLE** | Required to create export tables when setting up new data streams |
| **CREATE TYPE** | Required to create table-valued types used for efficient bulk data operations |

### Schema-Level Permissions

These permissions must be granted on the schema where export tables will be created (default: `dbo`):

| Permission | Purpose |
|------------|---------|
| **ALTER** | Required to create tables within the schema and modify table structure during upgrades |
| **EXECUTE** | Required to use table-valued types for bulk data operations |
| **SELECT** | Required to read data for existence checks and data retrieval |
| **INSERT** | Required to insert new records into export tables |
| **UPDATE** | Required to update existing records in Sync mode |
| **DELETE** | Required to remove records when entities are deleted in CluedIn |

### System Procedure Permissions

| Permission | Purpose |
|------------|---------|
| **EXECUTE on sp_rename** | Required for archive and rename operations on export streams |

### Information Schema Access

The connector also requires SELECT access to the following system views (typically granted by default):

- `INFORMATION_SCHEMA.TABLES` - Used to check if tables exist
- `INFORMATION_SCHEMA.COLUMNS` - Used to verify table structure
- `INFORMATION_SCHEMA.SCHEMATA` - Used to verify the target schema exists

## Permission Verification

When you test a connection in CluedIn, the connector automatically verifies that all required permissions are in place. If any permissions are missing, you will receive a detailed error message listing the specific permissions that need to be granted.

> **Note:** Permission verification only runs when you explicitly test a connection. It does not run during automatic health checks to avoid impacting existing installations.

## Alternative: Using db_owner Role

If fine-grained permissions are not required for your environment, you can simplify setup by adding the user to the `db_owner` role:

```sql
USE [YourDatabaseName];
GO

ALTER ROLE db_owner ADD MEMBER [CluedInConnector];
```

> **Warning:** The `db_owner` role grants full control over the database. Only use this option if your security policies allow it.

## Using a Custom Schema

If you prefer to isolate CluedIn export tables in a dedicated schema, you can create a custom schema and grant permissions on it:

```sql
-- Replace 'YourDatabaseName' with your target database name
-- Replace 'YourSchema' with your target schema (default is 'dbo')
-- Replace 'CluedInConnector' with your SQL user name

USE [YourDatabaseName];
GO

-- Create a dedicated schema for CluedIn exports
CREATE SCHEMA [YourSchema] AUTHORIZATION [dbo];
GO

-- Grant all required permissions on the custom schema
GRANT ALTER ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT EXECUTE ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT SELECT ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT INSERT ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT UPDATE ON SCHEMA::[YourSchema] TO [CluedInConnector];
GRANT DELETE ON SCHEMA::[YourSchema] TO [CluedInConnector];

-- Don't forget database-level permissions
GRANT CREATE TABLE TO [CluedInConnector];
GRANT CREATE TYPE TO [CluedInConnector];
```

Then configure the connector to use the `YourSchema` schema in the connection settings.

## Troubleshooting

### Common Permission Errors

| Error Message | Solution |
|---------------|----------|
| "CREATE TABLE permission denied" | Grant `CREATE TABLE` permission at the database level |
| "ALTER permission denied on schema" | Grant `ALTER` permission on the target schema |
| "EXECUTE permission denied on object '...Type'" | Grant `EXECUTE` permission on the target schema |
| "The INSERT permission was denied" | Grant `INSERT` permission on the target schema |

### Verifying Permissions

You can verify the permissions granted to a user by running:

```sql
-- Check database-level permissions
SELECT * FROM sys.fn_my_permissions(NULL, 'DATABASE');

-- Check schema-level permissions (replace 'YourSchema' with your schema name)
SELECT * FROM sys.fn_my_permissions('YourSchema', 'SCHEMA');
```

## Security Considerations

- **Principle of Least Privilege**: We recommend granting only the minimum required permissions listed above rather than using `db_owner`.
- **Dedicated User**: Consider creating a dedicated SQL user specifically for the CluedIn connector.
- **Custom Schema**: Using a custom schema helps isolate CluedIn tables and makes permission management easier.
- **Regular Audits**: Periodically review the permissions granted to the connector user to ensure they align with your security policies.

## Questions?

If you have questions about configuring SQL Server permissions for the CluedIn connector, please contact CluedIn support.
