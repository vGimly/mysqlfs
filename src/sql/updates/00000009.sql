-- Bogus BEGIN since TABLE definitions are not transaction-safe.
BEGIN;

-- Creating Table
CREATE TABLE `xattr` (
  `inode` BIGINT(2) NOT NULL,
  `attr` VARCHAR(64) NOT NULL COLLATE 'utf8mb4_bin',
  `value` BLOB NULL DEFAULT NULL,
  PRIMARY KEY (`inode`, `attr`) USING BTREE
)
COLLATE='utf8mb4_bin'
ENGINE=InnoDB
;

-- First record

-- Commit everything
COMMIT;
