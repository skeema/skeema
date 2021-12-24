SET foreign_key_checks=0;
SET sql_log_bin=0;
CREATE DATABASE testing;

use testing

# Keep this in sync with tengo_test.go's aTable()
CREATE TABLE actor (
	actor_id smallint(5) unsigned NOT NULL AUTO_INCREMENT,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) DEFAULT NULL,
	last_update timestamp/*!50601(2)*/ NOT NULL DEFAULT CURRENT_TIMESTAMP(/*!506012*/) ON UPDATE CURRENT_TIMESTAMP(/*!506012*/),
	ssn char(10) NOT NULL,
	alive tinyint(1) unsigned NOT NULL DEFAULT '1',
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
CREATE TABLE `followed_posts` (
  `post_id` bigint(20) unsigned NOT NULL,
  `user_id` bigint(20) unsigned NOT NULL,
  `subscribed_at` int(10) unsigned DEFAULT NULL,
  `metadata` text,
  PRIMARY KEY (`post_id`,`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
/*!50100 PARTITION BY RANGE (user_id)
SUBPARTITION BY HASH (post_id)
SUBPARTITIONS 2
(PARTITION p0 VALUES LESS THAN (123) ENGINE = InnoDB,
 PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;

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

CREATE TABLE grab_bag (
	id bigint unsigned NOT NULL AUTO_INCREMENT,
	owner_id int unsigned,
	name varchar(100) CHARACTER SET utf8 NOT NULL,
	code char(8) DEFAULT 'XYZ01234',
	updated_at timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	created_at timestamp/*!50601(2) DEFAULT CURRENT_TIMESTAMP(2)*/,
	alive tinyint(1) DEFAULT '1' COMMENT 'column comment',
	flags bit(8) DEFAULT b'1',
	metadata blob,
	PRIMARY KEY (id, code),
	UNIQUE KEY name_idx (name),
	KEY recency USING BTREE (updated_at, created_at),
	KEY owner_idx (owner_id) COMMENT 'index comment',
	/*!50601 FULLTEXT */ KEY ft_name (name),
	CONSTRAINT Ab FOREIGN KEY (id, code) REFERENCES sometable1 (somecol1a, somecol1b),
	CONSTRAINT _aa FOREIGN KEY (updated_at, created_at) REFERENCES sometable2 (somecol2a, somecol2b),
	CONSTRAINT cc FOREIGN KEY (name) REFERENCES sometable3 (somecol3),
	CONSTRAINT aa FOREIGN KEY (name) REFERENCES sometable3 (somecol3) ON UPDATE RESTRICT,
	CONSTRAINT bb FOREIGN KEY (name) REFERENCES sometable3 (somecol3) ON DELETE NO ACTION
) AUTO_INCREMENT=123 ROW_FORMAT=COMPACT CHECKSUM=1 DELAY_KEY_WRITE=1 COMMENT='hello';

# Routine definitions here are intentionally formatted oddly. The DB remembers
# formatting in some places but not others.

# Keep this in sync with tengo_test.go's aProc()
delimiter //
CREATE PROCEDURE proc1(
    IN name varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    INOUT iterations int(10) unsigned,   OUT pct decimal(5, 2)
) READS SQL DATA   SQL SECURITY  INVOKER
  BEGIN
  SELECT @iterations + 1, 98.76 INTO iterations, pct;
  END //
delimiter ;

# Keep this in sync with tengo_test.go's aFunc()
CREATE FUNCTION func1(mult float(10,2))
returns float deterministic NO SQL COMMENT 'hello world' return mult * 2.0;

CREATE FUNCTION func2(  num    int, 
    name   varchar(30) 
)
returns varchar(30) deterministic
return REPEAT(CONCAT('it''s ', name, '! '), num);

