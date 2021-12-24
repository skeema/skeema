SET foreign_key_checks=0;
SET sql_log_bin=0;

use testing

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

