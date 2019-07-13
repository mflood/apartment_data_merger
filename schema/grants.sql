-- Sets up postgres permissions for
-- DDL user and APP user

-- both can connect
GRANT CONNECT on DATABASE digible to ddl;
GRANT CONNECT on DATABASE digible to app;

-- DDL has all privs
GRANT ALL PRIVILEGES ON SCHEMA digible_schema TO ddl;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA digible_schema TO ddl;

-- APP only has CRUD privs
GRANT USAGE ON SCHEMA digible_schema TO app;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA digible_schema TO app;

