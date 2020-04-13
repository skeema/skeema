# Table with two fulltext indexes, incl one using ngram parser (which is
# preinstalled in MySQL 5.7+)

SET foreign_key_checks=0;
SET sql_log_bin=0;

use testing

CREATE TABLE ftparser (
	id int unsigned not null auto_increment,
	body varchar(2000),
	description varchar(1000),
	name varchar(30),
	PRIMARY KEY (id),
	FULLTEXT KEY ftdesc (description) WITH PARSER ngram,
	KEY name (name),
	FULLTEXT KEY ftbody (body)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

