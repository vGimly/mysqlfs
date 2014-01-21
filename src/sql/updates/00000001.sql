-- Bogus BEGIN since TABLE definitions are not transaction-safe.
BEGIN;

-- Creating Table
CREATE TABLE `SW_DETAILS` (
  `KEY` VARCHAR(200) NOT NULL,
  `VALUE` VARCHAR(200) NOT NULL,
  `LAST_CHANGE` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (`KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- First record
REPLACE INTO SW_DETAILS SET `KEY` = "VERSION", `VALUE` = "0.4.2beta";

-- Commit everything
COMMIT;
