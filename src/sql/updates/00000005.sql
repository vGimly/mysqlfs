-- Bogus BEGIN since TABLE definitions are not transaction-safe.
BEGIN;

-- UPDATE the SW_DETAILS to freeze this database version to
-- a specific mysqlfs version
REPLACE INTO SW_DETAILS SET `KEY` = "VERSION", `VALUE` = "0.4.2";

-- Commit everything
COMMIT;
