# Tables using MariaDB's check constraints

SET foreign_key_checks=0;
SET sql_log_bin=0;

use testing

CREATE TABLE `has_checks1` (
	`id` int unsigned NOT NULL auto_increment,
	`num1` int DEFAULT NULL CHECK (num1 < 100),
	`num2` int DEFAULT NULL,
	`num3` int DEFAULT NULL CHECK (num3 <> 0),
	`name` varchar(100),
	PRIMARY KEY (id),
	CONSTRAINT mult_cols CHECK (num1 < num2),
	CHECK (num3 >= num1),
	CONSTRAINT name_not_inline CHECK (name != 'bob\'s name' AND length(name) > 3)
);

CREATE TABLE has_checks2 (
	foo1 varchar(30) NOT NULL CHECK (foo1 <> 'hello world'),
	foo2 int unsigned NOT NULL CHECK (foo2 > 0),
	CONSTRAINT foo1 CHECK (foo1 <> 'C:\\foo\\bar'),
	CONSTRAINT foo2 CHECK (length(foo1) + foo2 != 123)
);
