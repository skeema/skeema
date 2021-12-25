# Advanced index features present in MySQL/Percona 8+

SET foreign_key_checks=0;
SET sql_log_bin=0;

use testing

CREATE TABLE my8idx (
	a int NOT NULL,
	b int,
	c int,
	d int,
	name varchar(50),
	PRIMARY KEY (a),
	INDEX idx (d, (b * c) DESC) INVISIBLE,
	INDEX test3b ((concat('$',name)), (concat('€', name))),
	INDEX test4b ((b * c), d, (concat('💩', name)))
);
