# This table uses the schema's default charset of latin1
CREATE TABLE badcsdef (
	id int unsigned NOT NULL,
	name varchar(30),
	PRIMARY KEY (id)
) ENGINE=InnoDB; -- bad charset will show up on this line due to reformat happening first

CREATE TABLE badcscol (
	id int unsigned NOT NULL,
	fine varchar(20) COLLATE utf8mb4_swedish_ci,
	name varchar(30) COLLATE latin1_general_ci,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE badcsmulti (
	id int unsigned NOT NULL,
	name varchar(30) CHARACTER SET latin1,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
