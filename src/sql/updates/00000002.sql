-- Bogus BEGIN since TABLE definitions are not transaction-safe.
BEGIN;

-- Add a column to store the block size
ALTER TABLE data_blocks
 ADD COLUMN datalength INT(8) UNSIGNED NOT NULL AFTER seq; 

-- Commit everything
COMMIT;
