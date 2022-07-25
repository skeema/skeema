# Tables using explicit InnoDB tablespace clauses (MySQL 5.7+)

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
