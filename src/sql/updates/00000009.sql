-- Bogus BEGIN since TABLE definitions are not transaction-safe.
BEGIN;

-- Creating Table
CREATE TABLE IF NOT EXISTS `xattr` (
  `inode` BIGINT(2) UNSIGNED NOT NULL,
  `attr` VARCHAR(64) NOT NULL COLLATE 'utf8mb4_bin',
  `value` BLOB NULL DEFAULT NULL,
  PRIMARY KEY (`inode`, `attr`) USING BTREE,
  CONSTRAINT `drop_xattr` FOREIGN KEY (`inode`) REFERENCES `inodes` (`inode`) ON UPDATE CASCADE ON DELETE CASCADE
)
COLLATE='utf8mb4_bin'
ENGINE=InnoDB
;

-- First record

-- Commit everything
COMMIT;
