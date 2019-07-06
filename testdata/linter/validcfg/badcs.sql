# This table uses the schema's default charset of latin1
CREATE TABLE badcsdef ( /* due to db default charset... annotations:charset */
	id int unsigned NOT NULL,
	name varchar(30),
	PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE TABLE badcscol (
	id int unsigned NOT NULL,
	fine varchar(20) COLLATE utf8mb4_swedish_ci,
	name varchar(30) COLLATE latin1_general_ci, /* annotations:charset */
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE badcsmulti (
	id int unsigned NOT NULL,
	name varchar(30) CHARACTER SET latin1,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 /* annotations:charset */;
