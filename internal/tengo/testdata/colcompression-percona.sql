# Table using Percona Server's column compression

SET foreign_key_checks=0;

use testing

CREATE TABLE colcompr(
	id int unsigned NOT NULL,
	body text character set utf8mb4 COLUMN_FORMAT COMPRESSED,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
