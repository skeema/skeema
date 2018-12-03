SET foreign_key_checks=0;
CREATE DATABASE testing;
CREATE DATABASE testcollate DEFAULT COLLATE latin1_bin;
CREATE DATABASE testcharset DEFAULT CHARACTER SET utf8mb4;
CREATE DATABASE testcharcoll DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

use testing

# Keep this in sync with tengo_test.go's aTable()
CREATE TABLE actor (
	actor_id smallint(5) unsigned NOT NULL AUTO_INCREMENT,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) DEFAULT NULL,
	last_update timestamp/*!50601(2)*/ NOT NULL DEFAULT CURRENT_TIMESTAMP(/*!506012*/) ON UPDATE CURRENT_TIMESTAMP(/*!506012*/),
	ssn char(10) NOT NULL,
	alive tinyint(1) NOT NULL DEFAULT '1',
	alive_bit bit(1) NOT NULL DEFAULT b'1',
	PRIMARY KEY (actor_id),
	UNIQUE KEY idx_ssn (ssn),
	KEY idx_actor_name (last_name(10),first_name(1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

# Keep this in sync with tengo_test.go's anotherTable()
CREATE TABLE actor_in_film (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

# Keep this in sync with tengo_test.go's unsupportedTable(), or make that
# function use a different unsupported feature once partitioning is supported
CREATE TABLE orders (
	id int unsigned NOT NULL AUTO_INCREMENT,
	customer_id int unsigned NOT NULL,
	info text,
	PRIMARY KEY (id, customer_id)
) ENGINE=InnoDB ROW_FORMAT=REDUNDANT PARTITION BY RANGE (customer_id) (
	PARTITION p0 VALUES LESS THAN (123),
	PARTITION p1 VALUES LESS THAN MAXVALUE
);

# Keep this table in sync with tengo_test.go's foreignKeyTable()
CREATE TABLE warranties (
  id int(10) unsigned NOT NULL,
  customer_id int(10) unsigned DEFAULT NULL,
  product_line char(12) NOT NULL,
  model int(10) unsigned NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY product (product_line,model),
  KEY customer (customer_id),
  CONSTRAINT customer_fk FOREIGN KEY (customer_id) REFERENCES purchasing.customers (id) ON DELETE SET NULL,
  CONSTRAINT product FOREIGN KEY (product_line, model) REFERENCES products (line, model) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE has_rows (
	id int unsigned NOT NULL AUTO_INCREMENT,
	name varchar(30),
	PRIMARY KEY (id)
);
INSERT INTO has_rows (name) VALUES
("Jimbo"),
("Fred"),
("Dolph"),
("Zorgon");
CREATE TABLE no_rows LIKE has_rows;

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

CREATE TABLE grab_bag (
	id bigint unsigned NOT NULL AUTO_INCREMENT,
	owner_id int unsigned,
	name varchar(100) NOT NULL,
	code char(8) DEFAULT 'XYZ01234',
	updated_at timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	created_at timestamp/*!50601(2) DEFAULT CURRENT_TIMESTAMP(2)*/,
	alive tinyint(1) DEFAULT '1' COMMENT 'column comment',
	flags bit(8) DEFAULT b'1',
	metadata blob,
	PRIMARY KEY (id, code),
	UNIQUE KEY name_idx (name),
	KEY recency (updated_at, created_at),
	KEY owner_idx (owner_id) COMMENT 'index comment',
	CONSTRAINT Ab FOREIGN KEY (id, code) REFERENCES sometable1 (somecol1a, somecol1b),
	CONSTRAINT _aa FOREIGN KEY (updated_at, created_at) REFERENCES sometable2 (somecol2a, somecol2b),
	CONSTRAINT aa FOREIGN KEY (name) REFERENCES sometable3 (somecol3)
) AUTO_INCREMENT=123;

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
) DEFAULT CHARSET=latin1;

CREATE TABLE many_permutations2 (
	a char(10),
	b char(10) CHARACTER SET latin1,
	c char(10) COLLATE latin1_swedish_ci,
	d char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	e char(10) COLLATE latin1_general_ci,
	f char(10) CHARACTER SET utf8mb4,
	g char(10) COLLATE utf8mb4_general_ci
) DEFAULT CHARSET=latin1 COLLATE latin1_general_ci;