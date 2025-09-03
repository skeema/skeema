# Table using MariaDB's column compression

use testing;

CREATE TABLE colcompr(
	id int unsigned NOT NULL,
	body text compressed=zlib character set utf8mb4,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
