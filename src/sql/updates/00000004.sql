-- Bogus BEGIN since TABLE definitions are not transaction-safe.
BEGIN;

-- Add an index to the tree table
CREATE INDEX `tree_name` ON `tree` (`name`);

-- Commit everything
COMMIT;
