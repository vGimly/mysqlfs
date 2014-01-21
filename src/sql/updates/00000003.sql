-- Bogus BEGIN since TABLE definitions are not transaction-safe.
BEGIN;

-- Prepopulate the data block size cache
UPDATE `data_blocks` SET `datalength` = OCTET_LENGTH(`data`);

-- Commit everything
COMMIT;
