# Tables using invisible columns -- split into a separate file due to only
# being supported in MariaDB 10.3+ and MySQL 8.0.23+.

# NOTE: This test case actually fails on versions before 10.3.18 and 10.4.8
# due to a MariaDB bug; see MDEV-20210

SET foreign_key_checks=0;
SET sql_log_bin=0;

use testing

CREATE TABLE invistest (
	id int unsigned NOT NULL invisible auto_increment,
	first_name varchar(40) NOT NULL,
	middle_name varchar(80),
	last_name varchar(40),
	invis_default timestamp default current_timestamp invisible,
	invis_gen int AS (LENGTH(first_name)) INVISIBLE COMMENT 'hello world',
	PRIMARY KEY (id)
);
