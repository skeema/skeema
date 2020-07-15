# Tables testing behavior of InnoDB page compression clauses in MySQL 5.7+
CREATE TABLE page_comp_zlib (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMPRESSION='zlib';

CREATE TABLE page_comp_lz4 (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMPRESSION='lz4';

CREATE TABLE page_comp_none (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMPRESSION='none';