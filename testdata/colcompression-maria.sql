# Table using MariaDB's column compression

SET foreign_key_checks=0;
SET sql_log_bin=0;

use testing

CREATE TABLE colcompr(
	id int unsigned NOT NULL,
	body text compressed=zlib character set utf8mb4,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
