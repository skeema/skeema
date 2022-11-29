# Tables using MariaDB's check constraints

SET foreign_key_checks=0;

use testing

CREATE TABLE `has_checks1` (
	`id` int unsigned NOT NULL auto_increment,
	`num1` int DEFAULT NULL CHECK (num1 < 100),
	`num2` int DEFAULT NULL,
	`num3` int DEFAULT NULL CHECK (num3 <> 0 AND num3 <> 1 AND num3 != 2 AND num3 <> 3 AND num3 <> 4 AND num3 != 5),
	`name` varchar(100),
	PRIMARY KEY (id),
	CONSTRAINT mult_cols CHECK (num1 < num2),
	CHECK (num3 >= num1),
	CONSTRAINT name_not_inline CHECK (name != 'bob\'s name or maybe some other name let\'s make this a long string too € to test it' AND length(name) > 3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE has_checks2 (
	foo1 varchar(30) NOT NULL CHECK (foo1 <> 'hello world this is intentionally a long string in order to test the logic for clauses exceeding 64 bytes'),
	foo2 int unsigned NOT NULL CHECK (foo2 > 0),
	CONSTRAINT foo1 CHECK (foo1 <> 'C:\\foo\\bar'),
	CONSTRAINT foo2 CHECK (length(foo1) + foo2 != 123)
);
