# Tables using check constraints -- split into a separate file due to only
# being tested/supported in recent versions.

# This file is only used in MySQL 8+; MariaDB testing is a separate file due to
# some syntactical differences.

use testing;

CREATE TABLE `has_checks1` (
	`id` int unsigned NOT NULL auto_increment,
	`num1` int DEFAULT NULL CHECK (num1 < 100),
	`num2` int DEFAULT NULL,
	`num3` int DEFAULT NULL CONSTRAINT inline_named CHECK (num3 <> 0),
	`name` varchar(100),
	PRIMARY KEY (id),
	CONSTRAINT mult_cols CHECK (num1 < num2) ENFORCED,
	CHECK (num3 >= num1) NOT ENFORCED,
	CONSTRAINT name_not_inline CHECK (name != 'bob\'s name' AND name != '💩💩💩💩💩' AND length(name) > 3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE has_checks2 (
	foo1 varchar(30) NOT NULL,
	foo2 int unsigned NOT NULL,
	UNIQUE KEY foo2 (foo2),
	CONSTRAINT foo1 FOREIGN KEY (foo1) REFERENCES other_tbl (foo1),
	CONSTRAINT foo2 FOREIGN KEY (foo2) REFERENCES other_tbl (foo2),
	CONSTRAINT foo1 CHECK (foo1 <> 'C:\\foo\\bar'),
	CONSTRAINT foo2 CHECK (foo2 != 123)
);