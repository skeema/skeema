# This open source package does not support views, but even so, presence of
# views generally shouldn't break functions in this package. This test file
# contains two views, to be used in tests that confirm behavior with views
# present.

use testing;

CREATE SQL SECURITY INVOKER VIEW view1 AS SELECT curdate() AS `current_date`;

CREATE ALGORITHM=MERGE DEFINER=`doesntexist`@`localhost` view view2 AS
SELECT *
FROM   actor
WHERE  alive = 1
WITH CHECK OPTION;

# allow test on DropTablesInSchema to use BulkDropOptions.OnlyIfEmpty
DELETE FROM has_rows;
