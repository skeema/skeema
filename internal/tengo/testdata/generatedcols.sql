# Tables using generated columns -- split into a separate file due to only
# being tested/supported in recent versions.

# This file is only used in MySQL 5.7+; MariaDB testing is a separate file due
# to lack of NOT NULL support in generated cols, and lack of 4-byte character
# support in generation expressions.

SET foreign_key_checks=0;
SET sql_log_bin=0;

use testing

CREATE TABLE staff (
	id int unsigned NOT NULL auto_increment,
	first_name varchar(40) NOT NULL,
	middle_name varchar(80),
	last_name varchar(40),
	full_name varchar(162) AS (CONCAT(first_name, ' ', middle_name, ' ', last_name, '€')) VIRTUAL COMMENT 'may be null if any elements are null!',
	full_name_nonull varchar(162) AS (CONCAT(first_name, ' ', IFNULL(middle_name, _utf8mb4'💩'), ' ', IFNULL(last_name, ''))) STORED NOT NULL,
	PRIMARY KEY (id),
	UNIQUE KEY name (full_name_nonull)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
