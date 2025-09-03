# Coverage for ignored (invisible) indexes in MariaDB 10.6

use testing;

CREATE TABLE maria106idx (
	a int NOT NULL,
	b int,
	c int,
	d int,
	PRIMARY KEY (a),
	INDEX idx (d, b) IGNORED
);
