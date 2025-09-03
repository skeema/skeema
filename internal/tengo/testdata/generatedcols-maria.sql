# Tables using generated columns -- split into a separate file due to only
# being tested/supported in recent versions.

# This file is only used in MariaDB 10.2+; it's separated from the MySQL test
# due to MariaDB not supporting NOT NULL in generated cols, and not supporting
# 4-byte characters in generation expressions.
# Also this package doesn't support MariaDB 10.1's alternative syntax for
# generated columns, so it is excluded from related tests.

use testing;

CREATE TABLE staff (
	id int unsigned NOT NULL auto_increment,
	first_name varchar(40) NOT NULL,
	middle_name varchar(80),
	last_name varchar(40),
	full_name varchar(162) AS (CONCAT(first_name, ' ', middle_name, ' ', last_name, '€')) VIRTUAL COMMENT 'hello world',
	full_name_nonull varchar(162) AS (CONCAT(first_name, ' ', IFNULL(middle_name, ''), ' ', IFNULL(last_name, ''))) STORED,
	PRIMARY KEY (id),
	KEY name (full_name_nonull)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
