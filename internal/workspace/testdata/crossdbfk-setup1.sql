# MySQL 8.0 extends metadata locks (MDL) across both sides of foreign keys. This
# file sets up tables in one schema, and then crossdbfk/tables.sql is used in a
# workspace which has child-side FKs with these tables on the parent side. This
# is used for testing lock_wait_timeout and resiliency to MDL locking conflicts.

CREATE DATABASE parent_side;
USE parent_side;

CREATE TABLE p1 (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(30),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

INSERT INTO p1 (name) VALUES ('hello world');

CREATE TABLE p2 (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(30),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

INSERT INTO p2 (name) VALUES ('hello world');
