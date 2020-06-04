CREATE TABLE uhoh (id int unsigned NOT NULL);

CREATE TABLE foo.tbl1 (id int unsigned NOT NULL);

# ignore-table should cause annotation for this one to be skipped!
CREATE TABLE abcd.ignoreme (id int unsigned NOT NULL);

use bar
CREATE TABLE tbl2 (id int unsigned NOT NULL);
