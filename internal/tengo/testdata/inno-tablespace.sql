# Tables using MySQL-only syntax for InnoDB tablespace and/or autoextend_size

USE testing;

CREATE TABLE explicit_tablespace_fpt (
	id int unsigned not null auto_increment,
	name varchar(30),
	primary key (id)
) tablespace = innodb_file_per_table auto_increment = 123;

CREATE TABLE explicit_tablespace_sys (
	id int unsigned not null auto_increment,
	name varchar(30),
	primary key (id)
) tablespace `innodb_system` auto_increment = 456;

CREATE TABLE autoext_only (
	id int unsigned not null,
	name varchar(30),
	primary key (id)
) autoextend_size=4194304 comment='test';

CREATE TABLE explicit_tablespace_fpt_autoext (
	id int unsigned not null auto_increment,
	name varchar(30),
	primary key (id)
) tablespace = innodb_file_per_table  autoextend_size=16M auto_increment = 765;
