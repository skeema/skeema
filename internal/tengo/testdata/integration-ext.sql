# This file contains additional definitions used by several tests, but not
# a majority of tests, so they're separated out from integration.sql.

CREATE DATABASE testcollate DEFAULT COLLATE latin1_bin;
CREATE DATABASE testcharset DEFAULT CHARACTER SET utf8mb4;
CREATE DATABASE testcharcoll DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

use testing;

CREATE TABLE no_indexes (
	foo varchar(50),
	price decimal(10, 2) DEFAULT '99.95'
);

CREATE TABLE no_pk (
	name varchar(80) DEFAULT 'a widget has no name',
	price decimal(10, 2) DEFAULT '99.95',
	index name_idx (name)
);

CREATE TABLE eww_myisam (
	id int unsigned NOT NULL AUTO_INCREMENT,
	PRIMARY KEY (id)
) ENGINE=MyISAM;

CREATE TABLE ft_test (
	id int unsigned not null auto_increment,
	body varchar(2000),
	PRIMARY KEY (id),
	/*!50601 FULLTEXT */ KEY ftbody (body)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

# MySQL 8 handles these incorrectly in information_schema, so this tests
# introspection from SHOW CREATE TABLE instead
CREATE TABLE bin_defaults (
	one binary(16) not null default 0x00,
	two binary(16) not null default 0x0001,
	three VARBinary(16) not null default 0x01077,
	four binary(16) not null default 0x01 COMMENT 'hello\'s world''s',
	five binary(16) not null default 0x77,
	six binary(16) not null default 0x7701,
	seven binary(16),
	eight binary(4) not null default ''
) ENGINE=InnoDB;

use testcharcoll

CREATE TABLE col_overrides_aplenty (
	one text CHARACTER SET latin1,
	two char(20) COLLATE utf8mb4_general_ci,
	three varchar(30) COLLATE latin1_bin
);

CREATE TABLE tbl_overrides (
	four mediumtext,
	five varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
	six char(10) COLLATE latin1_swedish_ci
) DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

# Particularly useful for testing the changes in MySQL 8.0
CREATE TABLE many_permutations1 (
	a char(10),
	b char(10) CHARACTER SET latin1,
	c char(10) COLLATE latin1_swedish_ci,
	d char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	e char(10) COLLATE latin1_general_ci,
	f char(10) CHARACTER SET utf8mb4,
	g char(10) COLLATE utf8mb4_general_ci
) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE many_permutations2 (
	a char(10),
	b char(10) CHARACTER SET latin1,
	c char(10) COLLATE latin1_swedish_ci,
	d char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	e char(10) COLLATE latin1_general_ci,
	f char(10) CHARACTER SET utf8mb4,
	g char(10) COLLATE utf8mb4_general_ci
) DEFAULT CHARSET=latin1 COLLATE latin1_general_ci;

CREATE TABLE many_permutations3 (
	a char(10),
	b char(10) CHARACTER SET latin1,
	c char(10) COLLATE latin1_swedish_ci,
	d char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	e char(10) COLLATE utf8_general_ci,
	f char(10) CHARACTER SET utf8mb3,
	g char(10) COLLATE utf8_unicode_ci
) DEFAULT CHARSET=utf8;

CREATE TABLE many_permutations4 (
	a char(10),
	b char(10) CHARACTER SET latin1,
	c char(10) COLLATE latin1_swedish_ci,
	d char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	e char(10) COLLATE utf8_general_ci,
	f char(10) CHARACTER SET utf8mb3,
	g char(10) COLLATE utf8_unicode_ci
) DEFAULT CHARSET=utf8mb3 COLLATE utf8_unicode_ci;
