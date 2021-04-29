# Table testing alias behavior of utf8mb3 --> utf8
# This test is run using just allow-charset=utf8mb3

CREATE TABLE badcscol (
	id int unsigned NOT NULL,
	fine varchar(20) COLLATE utf8mb4_swedish_ci, /* annotations:charset */
	name varchar(30),
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE badcstable (
	id int unsigned NOT NULL,
	name varchar(30) COLLATE utf8_unicode_ci,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /* annotations:charset */;
