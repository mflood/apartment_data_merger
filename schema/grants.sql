-- Sets up postgres permissions for
-- application user

GRANT CONNECT on DATABASE postgres to app_db_user;

-- app user only has CRUD privs
GRANT USAGE ON SCHEMA apartment_schema TO app_db_user;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA apartment_schema TO app_db_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA apartment_schema TO app_db_user;

