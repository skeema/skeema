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
	last_update timestamp(2) NOT NULL DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2),
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

# Keep this in sync with tengo_test.go's unsupportedTable()
CREATE TABLE actor_in_film_with_fk (
  actor_id smallint(5) unsigned NOT NULL,
  film_name varchar(60) NOT NULL,
  PRIMARY KEY (actor_id,film_name),
  KEY film_name (film_name),
  CONSTRAINT fk_actor_id FOREIGN KEY (actor_id) REFERENCES actor (actor_id)
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
	created_at timestamp(2) DEFAULT CURRENT_TIMESTAMP(2),
	updated_at timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	alive tinyint(1) DEFAULT '1' COMMENT 'column comment',
	flags bit(8) DEFAULT b'1',
	PRIMARY KEY (id, code),
	UNIQUE KEY name_idx (name),
	KEY recency (updated_at, created_at),
	KEY owner_idx (owner_id) COMMENT 'index comment'
) AUTO_INCREMENT=123;

CREATE TABLE partitioned (
	id int unsigned NOT NULL AUTO_INCREMENT,
	customer_id int unsigned NOT NULL,
	info text,
	PRIMARY KEY (id, customer_id)
) ENGINE=InnoDB ROW_FORMAT=REDUNDANT PARTITION BY RANGE (customer_id) (
	PARTITION p0 VALUES LESS THAN (123),
	PARTITION p1 VALUES LESS THAN MAXVALUE
);

use testcharcoll

CREATE TABLE col_overrides_aplenty (
	one text CHARACTER SET latin1,
	two char(20) COLLATE utf8mb4_general_ci,
	three varchar(30) COLLATE latin1_bin
);

CREATE TABLE tbl_overrides (
	four mediumtext,
	five varchar(45) CHARACTER SET utf8mb4,
	six char(10) COLLATE latin1_swedish_ci
) DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

